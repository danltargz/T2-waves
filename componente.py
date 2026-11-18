#!/usr/bin/env python3

# T2 - Waves (algoritmo do Eco)
# PD - PUCPR
# Prof. Luiz Lima Jr.
#
# Integrantes do Grupo:
# 1. Gustavo Jansen Butenas
# 2. André Esteves Arantes
# 3. André Luís Scharaiber Alves
# 4. Danillo Gonçalves Camargo da Silva
# 5. Rodrigo Schiavinatto Plassmann

from pika import BlockingConnection
from sys import argv
from enum import Enum

if len(argv) < 2:
    print(f"USO: {argv[0]} <id> [<v1> <v2> ...]")
    exit(1)

Estado = Enum('Estado', ['OCIOSO', 'INICIADOR', 'ATIVO', 'OK'])

idx = argv[1]
Nx = argv[2:]
estado = Estado.OCIOSO       # estado inicial

iniciador = False   # vira True só em quem recebe o sinal do starter
entrada = None      # vizinho que me mandou a primeira R
cont = 0            # quantos vizinhos já me mandaram R ou I
info = ""           # meu id, mais as informações I que forem chegando

print("meu id =", idx)
print("meus vizinhos =", Nx)

conexao = BlockingConnection()
canal = conexao.channel()

canal.queue_declare(queue=idx, durable=True, auto_delete=True)

# envia mensagem para dest
def envia(msg, dests):
    for dest in dests:
        canal.basic_publish(exchange="",
                            routing_key=dest,
                            body=f"{idx}:{msg}")

# quando todos os vizinhos já mandaram R ou I, a minha parte acabou
def verifica():
    global estado
    if cont == len(Nx):
        estado = Estado.OK
        if iniciador:
            print("entidades do sistema:", info)
        else:
            print(f"enviando I ({info}) para a entrada {entrada}")
            envia("I " + info, [entrada])

def recebendo(msg, origem):
    global estado, iniciador, entrada, cont, info
    if estado == Estado.OCIOSO:
        if msg == "R":
            print(f"R recebida de {origem}, que passa a ser a minha entrada")
            iniciador = False
            entrada = origem
            info = idx
            cont = 1
            Nxo = Nx[:]         # cópia dos vizinhos
            Nxo.remove(origem)
            envia("R", Nxo)
            estado = Estado.ATIVO
            verifica()          # se eu sou borda, já respondo aqui

    elif estado == Estado.ATIVO:
        if msg == "R":
            # R de um vizinho que eu esperava também conta, ele não vai mandar I
            print(f"R recebida de {origem}")
        else:
            print(f"{msg} recebida de {origem}")
            info = info + " " + msg[2:]     # tira o "I " da frente
        cont = cont + 1
        verifica()

def espontaneamente(msg):
    global estado, iniciador, cont, info
    if estado == Estado.OCIOSO:
        print("iniciador:", msg)
        estado = Estado.INICIADOR
        iniciador = True
        info = idx
        cont = 0
        envia("R", Nx)
        estado = Estado.ATIVO
        verifica()              # só termina aqui se eu não tiver vizinhos

def callback(canal, metodo,  props, corpo):
    msg = corpo.decode().split(":")
    if len(msg) < 2:
        # nao tem origem
        msg = msg[0]
        origem = "STARTER"
    else:
        origem = msg[0]     # a origem tem que sair antes de msg ser trocada
        msg = msg[1]

    if origem.upper()=="STARTER":
        espontaneamente(msg)
    else:
        recebendo(msg, origem)

canal.basic_consume(queue=idx,
                    on_message_callback=callback,
                    auto_ack=True)

try:
    print(f"{idx}: aguardando mensagens...")
    canal.start_consuming()
except KeyboardInterrupt:
    canal.stop_consuming()

conexao.close()
print("Terminando")
