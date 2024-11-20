import OllamaInit

EXIT = "exit"

def chat(input, use_cn_model):
	response = OllamaInit.client.chat(model=OllamaInit.model_cn if use_cn_model.lower() == 'y' else OllamaInit.model_en, messages=[
 		{
            'role': 'system',
            'content': "你是一个AI小助手",
        },
		 {
    		'role': 'user',
   		 	'content': input,
  		}
	])
	return response['message']['content']

use_cn_model = input("使用llama中文模型(y/n)：")
str = input("有什么可以帮你的，请输入：")
while str.lower() != EXIT:
	try:
		result = chat(str, use_cn_model)
		print(result, "\n")
		str = input("请输入：")
	except OllamaInit.ollama.ResponseError as e:
  		print('错误:', e.error)
  		str = EXIT
print("bye~")
