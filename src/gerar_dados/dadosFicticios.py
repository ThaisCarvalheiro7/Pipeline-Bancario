

"""
Gerador de dados sintéticos bancários: clientes, contas e transações.

Simula um extrato de banco com inconsistências propositais (nulos,
duplicatas, formatos de data variados) para praticar tratamento de
dados nas camadas Bronze/Silver/Gold do pipeline.
"""

import random
import uuid
from datetime import datetime, timedelta

import pandas as pd
from faker import Faker

fake = Faker("pt_BR")
Faker.seed(42)
random.seed(42)

QTD_CLIENTES = 500
QTD_CONTAS = 650
QTD_TRANSACOES = 20_000

TIPOS_TRANSACAO = ["PIX", "TED", "DOC", "CARTAO_CREDITO", "CARTAO_DEBITO", "BOLETO"]
FORMATOS_DATA = ["%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M", "%Y-%m-%dT%H:%M:%S"]


def gerar_clientes(qtd: int) -> pd.DataFrame:
    clientes = []
    for _ in range(qtd):
        clientes.append(
            {
                "id_cliente": str(uuid.uuid4()),
                "nome": fake.name(),
                "cpf": fake.cpf(),
                "data_nascimento": fake.date_of_birth(minimum_age=18, maximum_age=90),
                "email": fake.email() if random.random() > 0.03 else None,  # 3% nulo
                "cidade": fake.city(),
                "estado": fake.estado_sigla(),
            }
        )
    return pd.DataFrame(clientes)


def gerar_contas(clientes: pd.DataFrame, qtd: int) -> pd.DataFrame:
    contas = []
    ids_clientes = clientes["id_cliente"].tolist()
    for _ in range(qtd):
        contas.append(
            {
                "id_conta": str(uuid.uuid4()),
                "id_cliente": random.choice(ids_clientes),
                "agencia": str(random.randint(1000, 9999)),
                "tipo_conta": random.choice(["CORRENTE", "POUPANCA"]),
                "data_abertura": fake.date_between(start_date="-8y", end_date="today"),
                "saldo_inicial": round(random.uniform(0, 50_000), 2),
            }
        )
    return pd.DataFrame(contas)


def gerar_transacoes(contas: pd.DataFrame, qtd: int) -> pd.DataFrame:
    transacoes = []
    ids_contas = contas["id_conta"].tolist()

    for _ in range(qtd):
        tipo = random.choice(TIPOS_TRANSACAO)

        if random.random() < 0.02:
            valor = round(random.uniform(20_000, 200_000), 2)
        else:
            valor = round(random.uniform(5, 5_000), 2)

        if random.random() < 0.01:
            valor = -valor

        data_transacao = fake.date_time_between(start_date="-180d", end_date="now")
        formato = random.choice(FORMATOS_DATA) 

        transacoes.append(
            {
                "id_transacao": str(uuid.uuid4()),
                "id_conta_origem": random.choice(ids_contas),
                "id_conta_destino": random.choice(ids_contas),
                "tipo_transacao": tipo,
                "valor": valor,
                "data_transacao": data_transacao.strftime(formato),
                "canal": random.choice(["APP", "INTERNET_BANKING", "AGENCIA", "CAIXA_ELETRONICO"]),
            }
        )

    df = pd.DataFrame(transacoes)

    duplicatas = df.sample(frac=0.005, random_state=42)
    df = pd.concat([df, duplicatas], ignore_index=True)

    mask_saque = df["tipo_transacao"] == "CARTAO_DEBITO"
    df.loc[mask_saque.sample(frac=0.3, random_state=1).index, "id_conta_destino"] = None

    return df.sample(frac=1, random_state=42).reset_index(drop=True)  # embaralha


def main():
    print("Gerando clientes...")
    df_clientes = gerar_clientes(QTD_CLIENTES)

    print("Gerando contas...")
    df_contas = gerar_contas(df_clientes, QTD_CONTAS)

    print("Gerando transações...")
    df_transacoes = gerar_transacoes(df_contas, QTD_TRANSACOES)

    df_clientes.to_csv("clientes.csv", index=False)
    df_contas.to_csv("contas.csv", index=False)
    df_transacoes.to_csv("transacoes.csv", index=False)

    print(f"\nClientes:    {len(df_clientes):>7} linhas -> clientes.csv")
    print(f"Contas:      {len(df_contas):>7} linhas -> contas.csv")
    print(f"Transações:  {len(df_transacoes):>7} linhas -> transacoes.csv")


if __name__ == "__main__":
    main()