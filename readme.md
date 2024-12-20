### QueryArtisan

**1. Step 1**: In the "data" folder, there are two multimodal datasets. For other open-source datasets, please download them yourself.


**2. Step 2**: The "config" folder contains configuration files. Please create the corresponding "db" database file based on the dataset you are testing.


**3. Step 3:** The "optimization" folder contains optimizers, which are independent of the system code. Since the optimizer code is based on modifications to the PostgreSQL database source code, please adhere to PostgreSQL database usage guidelines.


**4. Step 4:** Before testing, please ensure that the data schema is imported into the optimizer. Otherwise, errors may occur.

   
**5. Step 5:** Once the token and LLM are set up, you can begin testing.

**Note:** Many configurations are optional, and many parameters are not enabled by default. You are free to configure them as needed.


If the code has any errors, you can add a issue or contact us: skyrise.heng@gmail.com

