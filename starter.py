#!/usr/bin/env python3

# AMQP Basico - starter
# EP - PUCPR
# Prof. Luiz Lima Jr.
#
# o "starter" envia uma mensagem "externa"
# a um ou mais componentes do sistema
# que passam entao a assumir o papel de
# iniciadores

from pika import BlockingConnection
from sys import argv

if len(argv) < 3:
	print(f"Uso: {argv[0]} <mensagem> <dest1> [<dest2> ...]")
	exit(1)

mensagem = argv[1]
destinos = argv[2:]

conexao = BlockingConnection()
canal = conexao.channel()

def envia(msg, dests):
	for d in dests:
		canal.queue_declare(queue=d, durable=True, auto_delete=True)
		canal.basic_publish(exchange="",
			routing_key=d,
			body=f"STARTER:{msg}")
		print(f'Mensagem "{mensagem}" enviada para {d}')

envia(mensagem, destinos)

conexao.close()
