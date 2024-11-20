import BaseConfig
import ollama

modelfile='''
FROM llama3
SYSTEM 你要完全用简体中文回答
'''

model_en = 'my_llama3'
model_cn = 'lgkt/llama3-chinese-alpaca'

ollama.create(model=model_en, modelfile=modelfile)
ollama.generate(model=model_en, prompt='天空是蓝色的因为瑞利散射')

client = ollama.Client(host='http://localhost:11434', timeout=5000)

def default_print():
	print("你好，Python")
