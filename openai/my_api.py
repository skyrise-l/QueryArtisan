# -*- coding: utf-8 -*-

import json
from datetime import datetime as dt
from os import getenv
from requests import Response
import requests
from certifi import where


class MyApi:
    DEFAULT_SYSTEM_PROMPT = "You are a data analyst who will generate a logical plan for data analysis based on the data information, queries and instructions given by me, and further generate Python data analysis code according to the instructions."
    TITLE_PROMPT = "Generate a brief title for our conversation."

    def __init__(
        self,
        api_key,
        row_id,
        model="gpt-3.5-turbo-16k-0613",
      #  model="gpt-4-1106-preview",
        proxy= "http://127.0.0.1:7890",
    ):
        self.api_key = api_key
        self.system_prompt = getenv("API_SYSTEM_PROMPT", self.DEFAULT_SYSTEM_PROMPT)
        self.messages = self.init_message()
        self.conversation_id = 0
        self.model_slug = model
        self.id = row_id

        self.req_kwargs = {
            "proxies": {
                "http": proxy,
                "https": proxy,
            }
            if proxy
            else None,
            "verify": where(),
            "timeout": 600,
            "allow_redirects": False,
        }

    def init_message(self):
        messages = [{"role": "system", "content": self.DEFAULT_SYSTEM_PROMPT}]

        return messages

    def __get_headers(self, api_key):
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        }
        return headers

    def talk(self, prompt, TARGET_URL):
        user_prompt = {"role": "user", "content": prompt}
        self.messages.append(user_prompt)
        data = {
            "model": self.model_slug,
            "messages": self.messages,
        }

        url = TARGET_URL 

        resp = requests.post(
            url=url,
            headers=self.__get_headers(self.api_key),
            data=json.dumps(data),
            **self.req_kwargs,
        )

        return self.deal_reply(resp.status_code, json.loads(resp.text), 1)
    
    def talk_new(self, prompt, TARGET_URL):
        user_prompt = {"role": "user", "content": prompt}
        tmpMessages = self.messages.copy()
        tmpMessages.append(user_prompt)
        data = {
            "model": self.model_slug,
            "messages": tmpMessages,
        }

        url = TARGET_URL 

        resp = requests.post(
            url=url,
            headers=self.__get_headers(self.api_key),
            data=json.dumps(data),
            **self.req_kwargs,
        )

        return self.deal_reply(resp.status_code, json.loads(resp.text), 0)
    
    def deal_reply(self, status, response, flag):
        if status != 200:
            print(f"访问API出现异常，建议查询当前情况：\nrow id :{str(self.id)}\n异常key: {self.api_key}")
            print(f"响应内容: {response}")
            return "rate limit"

        choice = response["choices"][0]
        
        if "message" in choice:
            text = choice["message"].get("content", "")
        elif "delta" in choice:
            text = choice["delta"].get("content", "")
        else:
            text = "[ERROR] API response format not recognized."

        if flag == 1:
            self.messages.append({"role": "assistant", "content": text})

        return text
