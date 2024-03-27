# -*- coding: utf-8 -*-

import json
from datetime import datetime as dt
from os import getenv
import os
import openai

from requests import Response
import requests
from certifi import where
from .api_key_base import Conversations, UserPrompt, Prompt, SystemPrompt


class TurboGPT:
    DEFAULT_SYSTEM_PROMPT = 'You are a data analyst who will generate a logical plan for data analysis based on the data information, queries and instructions given by me, and further generate Python data analysis code according to the instructions.'
    TITLE_PROMPT = 'Generate a brief title for our conversation.'

    def __init__(self, api_keys: dict, proxy="http://127.0.0.1:7890"):
        self.api_keys = api_keys
        self.api_keys_key_list = list(api_keys)
        self.default_api_keys_key = self.api_keys_key_list[0]
        self.conversations_map = {}
        self.system_prompt = getenv('API_SYSTEM_PROMPT', self.DEFAULT_SYSTEM_PROMPT)
        
        self.req_kwargs = {
            'proxies': {
                'http': proxy,
                'https': proxy,
            } if proxy else None,
            'verify': where(),
            'timeout': 600,
            'allow_redirects': False,
        }

    def __get_conversations(self, api_keys_key=None):
        if api_keys_key is None:
            api_keys_key = self.default_api_keys_key

        if api_keys_key not in self.conversations_map:
            self.conversations_map[api_keys_key] = Conversations()

        return self.conversations_map[api_keys_key]

    def get_access_token(self, token_key=None):
        return self.api_keys[token_key or self.default_api_keys_key]

    def talk(self, content, model, message_id, parent_message_id, conversation_id=None, token=None):
        status = 0
        if conversation_id:
            conversation = self.__get_conversations(token).get(conversation_id)
            if not conversation:
                return self.__out_error_stream('Conversation not found', 404)

            parent = conversation.get_prompt(parent_message_id)
        else:
            conversation = self.__get_conversations(token).new()
            parent = conversation.add_prompt(Prompt(parent_message_id))
            parent = conversation.add_prompt(SystemPrompt(self.system_prompt, parent))

        conversation.add_prompt(UserPrompt(message_id, content, parent))

        user_prompt, gpt_prompt, messages = conversation.get_messages(message_id, model)
         
        print(self.__generate_json(model, messages))
        status, headers, response = self.request(self.get_access_token(token), self.__generate_json(model, messages))

        return status, headers, self.__map_conversation(status, conversation, gpt_prompt, response)


    @staticmethod
    def __out_stream(conversation, prompt, end=True):
        return {
            'message': prompt.get_message(end),
            'conversation_id': conversation.conversation_id,
            'error': None,
        }


    @staticmethod
    def __get_completion(status, data):
        if status != 200:
            error = data['error']['message'] if 'error' in data else 'Unknown error'
            result = {
                'detail': error
            }
            return False, result

        choice = data['choices'][0]
        if 'message' in choice:
            text = choice['message'].get('content', '')
        else:
            text = choice['delta'].get('content', '')

        return True, text

    def __map_conversation(self, status, conversation, gpt_prompt, data):
        success, result = self.__get_completion(status, data)
        if not success:
            return result

        choice = data['choices'][0]
        is_stop = 'stop' == choice['finish_reason']

        return self.__out_stream(conversation, gpt_prompt.append_content(result), is_stop)

    def __get_headers(self, api_key):
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}"
        }   
        return headers
    
    def __generate_json(self, model, messages):
        data = {
            'model': model,
            'messages': messages,
        }
        
        return data
    
    def request(self, api_key, data):
    
        url = 'https://api.openai.com/v1/chat/completions'
        #print(data)
       
        resp = requests.post(url=url, headers=self.__get_headers(api_key), data=json.dumps(data), **self.req_kwargs)

    
        return resp.status_code, resp.headers, json.loads(resp.text)