#############################################################################
#                      ATUALIZADOR DO BANCO DE DADOS DO SULTS               #
#                                                                           #
# Capta os dados do Sults via API e envia para o banco da Drogamais         #
# Versão 1.3.0                                                              #
# DATA DA ÚLTIMA ATUALIZAÇÃO: 04/12/2025                                    #
# DESENVOLVEDOR: GABRIEL CARVALHO DO ESPÍRITO SANTO E GIAN                  #
#############################################################################

import requests
import pandas as pd
import mariadb as mdb
from datetime import datetime
from config import DB_CONFIG, headers

lista_start = [0, 1, 2, 3]
agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

############# GERAIS ##############
def pegarIDs(Tabela, nome):

    print(f"Pegando ids da {nome}...")

    conn = mdb.connect(**DB_CONFIG)

    cursor = conn.cursor()

    cursor.execute(f"SELECT id FROM {nome}")
    ids_existentes = set(row[0] for row in cursor.fetchall())

    conn.commit()
    cursor.close()
    conn.close()

    update = Tabela[Tabela["id"].isin(ids_existentes)].copy()
    insert = Tabela[~Tabela["id"].isin(ids_existentes)].copy()
    ids_atuais = set(Tabela["id"])
    ids_para_deletar = [id_ for id_ in ids_existentes if id_ not in ids_atuais]
    delete = pd.DataFrame({"id": ids_para_deletar})

    return update, insert, delete


def apagar(tabela, nome):

    print(f"Apagando ids da tabela {nome}...")

    conn = mdb.connect(**DB_CONFIG)

    cursor = conn.cursor()
    for _, row in tabela.iterrows():
        id_tabela = int(row["id"])
        sql = f"DELETE FROM {nome} WHERE id = %s"
        cursor.execute(sql, (id_tabela,))

    conn.commit()
    cursor.close()
    conn.close()


############ PROJETOS #############
def buscarProjetos():

    print("Buscando Projetos...")

    def get_page(start):
        url = "https://api.sults.com.br/api/v1/projeto"
        params = {"start": str(start)}

        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            return data.get("data", [])
        except Exception as e:
            print(f"Erro na página {start}: {e}")
            return []

    todas_paginas = [get_page(start) for start in lista_start]
    dados_combinados = [item for pagina in todas_paginas for item in pagina]

    df = pd.json_normalize(dados_combinados)

    return df


def tratarProjetos(df):

    print("Tratando Projetos...")

    Projetos = df[
        [
            "id",
            "nome",
            "ativo",
            "pausado",
            "concluido",
            "dtCriacao",
            "dtInicio",
            "dtFim",
            "modelo.nome",
            "modelo.id",
            "responsavel.nome",
            "responsavel.id",
        ]
    ].copy()
    Projetos = Projetos.rename(
        columns={
            "modelo.nome": "modelo_nome",
            "modelo.id": "modelo_id",
            "responsavel.nome": "responsavel_nome",
            "responsavel.id": "responsavel_id",
        }
    )

    format_str = "%Y-%m-%dT%H:%M:%SZ"

    for col in ["dtCriacao", "dtInicio", "dtFim"]:
        Projetos[col] = pd.to_datetime(
            Projetos[col], format=format_str, errors="coerce"
        )

    for col in ["dtCriacao", "dtInicio", "dtFim"]:
        Projetos[col] = Projetos[col].dt.strftime("%Y-%m-%d %H:%M:%S")

    Projetos = Projetos.astype("object").where(Projetos.notna(), None)

    return Projetos


def atualizarProjetos(projetos_update):

    print("Atualizando Projetos...")

    conn = mdb.connect(**DB_CONFIG)

    cursor = conn.cursor()

    for _, row in projetos_update.iterrows():
        sql = """
        UPDATE tb_projetos
        SET nome = %s,
            ativo = %s,
            pausado = %s,
            concluido = %s,
            dtCriacao = %s,
            dtInicio = %s,
            dtFim = %s,
            modelo_nome = %s,
            modelo_id = %s,
            responsavel_nome = %s,
            responsavel_id = %s,
            data_atualizacao = NOW()
        WHERE id = %s
        """
        
        cursor.execute(
            sql,
            (
                row["nome"],
                row["ativo"],
                row["pausado"],
                row["concluido"],
                row["dtCriacao"],
                row["dtInicio"],
                row["dtFim"],
                row["modelo_nome"],
                row["modelo_id"],
                row["responsavel_nome"],
                row["responsavel_id"],
                row["id"],
            ),
        )

    conn.commit()
    cursor.close()
    conn.close()


def inserirProjetos(projetos_insert):

    print("Inserindo novos Projetos...")

    conn = mdb.connect(**DB_CONFIG)

    cursor = conn.cursor()

    for _, row in projetos_insert.iterrows():
        sql = """
        INSERT INTO tb_projetos (
            id, nome, ativo, pausado, concluido,
            dtCriacao, dtInicio, dtFim,
            modelo_nome, modelo_id,
            responsavel_nome, responsavel_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(
            sql,
            (
                row["id"],
                row["nome"],
                row["ativo"],
                row["pausado"],
                row["concluido"],
                row["dtCriacao"],
                row["dtInicio"],
                row["dtFim"],
                row["modelo_nome"],
                row["modelo_id"],
                row["responsavel_nome"],
                row["responsavel_id"],
            ),
        )

    conn.commit()
    cursor.close()
    conn.close()


############ TAREFAS #############
def buscarTarefas(projetos_ids):

    print("Buscando tarefas...")

    def get_tarefas(projeto_id):

        url = f"https://api.sults.com.br/api/v1/projeto/{projeto_id}/tarefa"

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json().get("data", [])

            for tarefa in data:
                tarefa["projetoId"] = projeto_id
            return data

        except Exception as e:
            print(f"Erro ao buscar projeto {projeto_id}: {e}")
            return []

    todas_tarefas = []
    for projeto_id in projetos_ids:
        tarefas = get_tarefas(projeto_id)
        todas_tarefas.extend(tarefas)

    tabela_final = pd.json_normalize(todas_tarefas)
    return tabela_final


def tratarTarefas(df):

    print("Tratando Tarefas...")

    df = df[df["fase.nome"].astype(str).str.len() <= 50].copy()
    df["descricaoHtml"] = (
        df["descricaoHtml"]
        .str.replace("<p>", "", regex=False)
        .str.replace("</p>", "", regex=False)
        .str.replace("<ul>", "", regex=False)
        .str.replace("<li>", "", regex=False)
        .str.replace("</ul>", "", regex=False)
        .str.replace("</li>", "", regex=False)
    )

    Tarefas = df[
        [
            "id",
            "nome",
            "descricaoHtml",
            "dtCriacao",
            "dtInicio",
            "dtFim",
            "dtConclusao",
            "projetoId",
            "fase.id",
            "fase.nome",
            "responsavel.id",
            "responsavel.nome",
        ]
    ].copy()
    Tarefas = Tarefas.rename(
        columns={
            "fase.id": "fase_id",
            "fase.nome": "fase_nome",
            "descricaoHtml": "descricao",
            "responsavel.id": "responsavel_id",
            "responsavel.nome": "responsavel_nome",
        }
    )

    format_str = "%Y-%m-%dT%H:%M:%SZ"
    format_str = "%Y-%m-%dT%H:%M:%SZ"

    for col in ["dtCriacao", "dtInicio", "dtFim", "dtConclusao"]:
        Tarefas[col] = pd.to_datetime(Tarefas[col], format=format_str, errors="coerce")

    for col in ["dtCriacao", "dtInicio", "dtFim", "dtConclusao"]:
        Tarefas[col] = Tarefas[col].dt.strftime("%Y-%m-%d %H:%M:%S")

    Tarefas = Tarefas.astype("object").where(Tarefas.notna(), None)

    return Tarefas


def atualizarTarefas(tarefas_update):

    print("Atualizando Tarefas...")

    conn = mdb.connect(**DB_CONFIG)

    cursor = conn.cursor()

    for _, row in tarefas_update.iterrows():
        sql = """
        UPDATE tb_tarefas
        SET nome = %s,
            descricao = %s,
            dtCriacao = %s,
            dtInicio = %s,
            dtFim = %s,
            dtConclusao = %s,
            projeto_id = %s,
            fase_id = %s,
            fase_nome = %s,
            responsavel_nome = %s,
            responsavel_id = %s,
            data_atualizacao = NOW()
        WHERE id = %s
        """
        cursor.execute(
            sql,
            (
                row["nome"],
                row["descricao"],
                row["dtCriacao"],
                row["dtInicio"],
                row["dtFim"],
                row["dtConclusao"],
                row["projetoId"],
                row["fase_id"],
                row["fase_nome"],
                row["responsavel_nome"],
                row["responsavel_id"],
                row["id"],
            ),
        )

    conn.commit()
    cursor.close()
    conn.close()


def inserirTarefas(tarefas_insert):

    print("Inserindo novas Tarefas...")

    conn = mdb.connect(**DB_CONFIG)

    cursor = conn.cursor()

    for _, row in tarefas_insert.iterrows():
        sql = """
        INSERT INTO tb_tarefas (
            id, nome, descricao, dtCriacao, dtInicio, dtFim, dtConclusao,
            projeto_id, fase_id, fase_nome,
            responsavel_nome, responsavel_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(
            sql,
            (
                row["id"],
                row["nome"],
                row["descricao"],
                row["dtCriacao"],
                row["dtInicio"],
                row["dtFim"],
                row["dtConclusao"],
                row["projetoId"],
                row["fase_id"],
                row["fase_nome"],
                row["responsavel_nome"],
                row["responsavel_id"],
            ),
        )

    conn.commit()
    cursor.close()
    conn.close()


############ LEADS #############
def buscarLeads():

    print("Buscando Leads...")

    def get_page(start):
        url = "https://api.sults.com.br/api/v1/expansao/negocio"
        params = {"start": str(start)}

        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            return data.get("data", [])
        except Exception as e:
            print(f"Erro na página {start}: {e}")
            return []

    todas_paginas = [get_page(start) for start in lista_start]
    dados_combinados = [item for pagina in todas_paginas for item in pagina]

    df = pd.json_normalize(dados_combinados)

    return df


def tratarLeads(df):

    print("Tratando Leads...")

    Etiquetas = df.explode("etiqueta")

    Leads = Etiquetas[
        [
            "id",
            "titulo",
            "descricao",
            "dtCadastro",
            "dtConclusao",
            "cidade",
            "uf",
            "valor",
            "situacaoPerdaMotivoObservacao",
            "situacaoPerdaMotivo.id",
            "situacaoPerdaMotivo.nome",
            "situacao.id",
            "situacao.nome",
            "situacaoPerdaMotivo.descricao",
            "etapa.id",
            "etapa.nome",
            "etapa.funil.id",
            "etapa.funil.nome",
            "campanha",
            "origem.id",
            "origem.nome",
            "temperatura.id",
            "temperatura.nome",
            "responsavel.id",
            "responsavel.nome",
            "etiqueta",
        ]
    ]

    etiquetas_expandido = Leads["etiqueta"].apply(pd.Series).add_prefix("etiqueta.")

    Leads = pd.concat([Leads.drop(columns=["etiqueta"]), etiquetas_expandido], axis=1)

    Leads.columns = Leads.columns.str.replace(".", "_", regex=False)

    format_str = "%Y-%m-%dT%H:%M:%SZ"
    format_str = "%Y-%m-%dT%H:%M:%SZ"

    for col in ["dtCadastro", "dtConclusao"]:
        Leads[col] = pd.to_datetime(Leads[col], format=format_str, errors="coerce")

    for col in ["dtCadastro", "dtConclusao"]:
        Leads[col] = Leads[col].dt.strftime("%Y-%m-%d %H:%M:%S")

    Leads = Leads.astype("object").where(Leads.notna(), None)

    return Leads


def atualizarLeads(leads_update):

    print("Atualizando Leads...")

    conn = mdb.connect(**DB_CONFIG)

    cursor = conn.cursor()

    for _, row in leads_update.iterrows():
        sql = """
        UPDATE tb_leads
        SET titulo = %s, 
            descricao = %s, 
            dtCadastro = %s,
            dtConclusao = %s,
            cidade = %s,
            uf = %s, 
            valor = %s, 
            situacaoPerdaMotivoObservacao = %s, 
            situacaoPerdaMotivo_id = %s,
            situacaoPerdaMotivo_nome = %s, 
            situacao_id = %s, 
            situacao_nome = %s,
            situacaoPerdaMotivo_descricao = %s,
            etapa_id = %s,
            etapa_nome = %s, 
            etapa_funil_id = %s,
            etapa_funil_nome = %s,
            campanha = %s,
            origem_id = %s,
            origem_nome = %s,
            temperatura_id = %s,
            temperatura_nome = %s,
            responsavel_id = %s, 
            responsavel_nome = %s,
            etiqueta_id = %s,
            etiqueta_nome = %s,
            etiqueta_cor = %s
        WHERE id = %s
        """
        cursor.execute(
            sql,
            (
                row["titulo"],
                row["descricao"],
                row["dtCadastro"],
                row["dtConclusao"],
                row["cidade"],
                row["uf"],
                row["valor"],
                row["situacaoPerdaMotivoObservacao"],
                row["situacaoPerdaMotivo_id"],
                row["situacaoPerdaMotivo_nome"],
                row["situacao_id"],
                row["situacao_nome"],
                row["situacaoPerdaMotivo_descricao"],
                row["etapa_id"],
                row["etapa_nome"],
                row["etapa_funil_id"],
                row["etapa_funil_nome"],
                row["campanha"],
                row["origem_id"],
                row["origem_nome"],
                row["temperatura_id"],
                row["temperatura_nome"],
                row["responsavel_id"],
                row["responsavel_nome"],
                row["etiqueta_id"],
                row["etiqueta_nome"],
                row["etiqueta_cor"],
                row["id"],
            ),
        )


def inserirLeads(leads_insert):

    print("Inserindo novos Leads...")

    conn = mdb.connect(**DB_CONFIG)

    cursor = conn.cursor()

    for _, row in leads_insert.iterrows():
        sql = """
        INSERT INTO tb_leads(
            id, titulo, descricao, dtCadastro, dtConclusao, cidade, uf, valor, situacaoPerdaMotivoObservacao, situacaoPerdaMotivo_id, 
            situacaoPerdaMotivo_nome, situacao_id, situacao_nome, situacaoPerdaMotivo_descricao, etapa_id, etapa_nome, etapa_funil_id, 
            etapa_funil_nome, campanha, origem_id, origem_nome, temperatura_id, temperatura_nome, responsavel_id, responsavel_nome, 
            etiqueta_id, etiqueta_nome, etiqueta_cor
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(
            sql,
            (
                row["id"],
                row["titulo"],
                row["descricao"],
                row["dtCadastro"],
                row["dtConclusao"],
                row["cidade"],
                row["uf"],
                row["valor"],
                row["situacaoPerdaMotivoObservacao"],
                row["situacaoPerdaMotivo_id"],
                row["situacaoPerdaMotivo_nome"],
                row["situacao_id"],
                row["situacao_nome"],
                row["situacaoPerdaMotivo_descricao"],
                row["etapa_id"],
                row["etapa_nome"],
                row["etapa_funil_id"],
                row["etapa_funil_nome"],
                row["campanha"],
                row["origem_id"],
                row["origem_nome"],
                row["temperatura_id"],
                row["temperatura_nome"],
                row["responsavel_id"],
                row["responsavel_nome"],
                row["etiqueta_id"],
                row["etiqueta_nome"],
                row["etiqueta_cor"],
            ),
        )

    conn.commit()
    cursor.close()
    conn.close()


############ TIMELINES #############
def buscarTimelines(leads_ids):

    print("Buscando Timelines...")

    def get_timelines(lead_id):

        url = f"https://api.sults.com.br/api/v1/expansao/negocio/{lead_id}/timeline"

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json().get("data", [])

            for timeline in data:
                timeline["negocioId"] = lead_id
            return data

        except Exception as e:
            print(f"Erro ao buscar lead {lead_id}: {e}")
            return []

    todas_timelines = []
    for lead_id in leads_ids:
        timelines = get_timelines(lead_id)
        todas_timelines.extend(timelines)

    tabela_final = pd.json_normalize(todas_timelines)

    return tabela_final


def tratarTimelines(tabela_final):

    print("Tratando Timelines...")

    Timelines = tabela_final[
        [
            "criado",
            "tipo",
            "negocioId",
            "pessoa.id",
            "pessoa.nome",
            "anotacao.id",
            "anotacao.descricaoHtml",
            "anotacao.dtAnotacao",
            "anotacao.editavel",
        ]
    ].copy()
    Timelines.columns = Timelines.columns.str.replace(".", "_", regex=False)

    col = ["criado", "anotacao_dtAnotacao"]

    Timelines[col] = Timelines[col].apply(
        pd.to_datetime, format="%Y-%m-%dT%H:%M:%SZ", errors="coerce"
    )
    for c in col:
        Timelines[c] = Timelines[c].dt.strftime("%Y-%m-%d %H:%M:%S")

    Timelines = Timelines.astype("object").where(Timelines.notna(), None)

    Timelines["id"] = Timelines["criado"].astype(str) + Timelines["negocioId"].astype(
        str
    )
    Timelines["id"] = (
        Timelines["id"]
        .str.replace("-", "", regex=False)
        .str.replace(":", "", regex=False)
        .str.replace(" ", "", regex=False)
        .astype(int)
    )

    Timelines["anotacao_descricaoHtml"] = (
        Timelines["anotacao_descricaoHtml"]
        .str.replace("<p>", " ", regex=False)
        .str.replace("</p>", " ", regex=False)
        .str.replace("<u>", " ", regex=False)
        .str.replace("</u>", " ", regex=False)
        .str.replace("<ul>", " ", regex=False)
        .str.replace("<li>", " ", regex=False)
        .str.replace("</ul>", " ", regex=False)
        .str.replace("</li>", " ", regex=False)
        .str.replace("<strong>", "", regex=False)
        .str.replace("</strong>", "", regex=False)
    )

    return Timelines


def atualizarTimelines(timelines_update):

    print("Atualizando Timelines...")

    conn = mdb.connect(**DB_CONFIG)

    cursor = conn.cursor()

    for _, row in timelines_update.iterrows():
        sql = """
        UPDATE tb_timelines
        SET criado = %s, tipo = %s, negocioId = %s, pessoa_id = %s, pessoa_nome = %s, anotacao_id = %s, anotacao_descricaoHtml = %s,
        anotacao_dtAnotacao = %s, anotacao_editavel = %s
        WHERE id = %s
        """
        cursor.execute(
            sql,
            (
                row["criado"],
                row["tipo"],
                row["negocioId"],
                row["pessoa_id"],
                row["pessoa_nome"],
                row["anotacao_id"],
                row["anotacao_descricaoHtml"],
                row["anotacao_dtAnotacao"],
                row["anotacao_editavel"],
                row["id"],
            ),
        )

    conn.commit()
    cursor.close()
    conn.close()


def inserirTimelines(timelines_insert):

    print("Inserindo novas Timelines...")

    conn = mdb.connect(**DB_CONFIG)

    cursor = conn.cursor()

    for _, row in timelines_insert.iterrows():
        sql = """
        INSERT INTO tb_timelines (
            id, criado, tipo, negocioId, pessoa_id, pessoa_nome, anotacao_id, anotacao_descricaoHtml, anotacao_dtAnotacao, anotacao_editavel
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(
            sql,
            (
                row["id"],
                row["criado"],
                row["tipo"],
                row["negocioId"],
                row["pessoa_id"],
                row["pessoa_nome"],
                row["anotacao_id"],
                row["anotacao_descricaoHtml"],
                row["anotacao_dtAnotacao"],
                row["anotacao_editavel"],
            ),
        )

    conn.commit()
    cursor.close()
    conn.close()


############ AVALIACAO #############
def buscarAvaliacao():

    print("Buscando Avaliações...")

    def get_page(start):
        url = "https://api.sults.com.br/api/v1/checklist/avaliacao"
        params = {"start": str(start)}

        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            return data.get("data", [])
        except Exception as e:
            print(f"Erro na página {start}: {e}")
            return []

    todas_paginas = [get_page(start) for start in lista_start]
    dados_combinados = [item for pagina in todas_paginas for item in pagina]

    avaliacoes = pd.json_normalize(dados_combinados)

    print("Tratando Avaliações...")

    avaliacoes = avaliacoes[avaliacoes["id"] >= 193]

    avaliacoes["id"] = avaliacoes["id"].astype(str)
    avaliacoes["loja_numero"] = avaliacoes["id"].str[:3]
    avaliacoes["id"] = avaliacoes["id"].astype(int)
    avaliacoes["id"] = avaliacoes["loja_numero"].astype(int)
    avaliacoes_ids = avaliacoes["id"].astype(str).tolist()

    format_str = "%Y-%m-%dT%H:%M:%SZ"
    format_str = "%Y-%m-%dT%H:%M:%SZ"

    for col in ["dtInicio", "dtFim", "dtCriacao", "dtPrazo"]:
        avaliacoes[col] = pd.to_datetime(
            avaliacoes[col], format=format_str, errors="coerce"
        )

    for col in ["dtInicio", "dtFim", "dtCriacao", "dtPrazo"]:
        avaliacoes[col] = avaliacoes[col].dt.strftime("%Y-%m-%d %H:%M:%S")

        avaliacoes = avaliacoes[
            [
                "id",
                "loja_numero",
                "dtInicio",
                "dtFim",
                "dtCriacao",
                "dtPrazo",
                "modelo.id",
                "modelo.nome",
                "responsavel.id",
                "responsavel.nome",
                "pontuacaoAlcancada",
                "pontuacaoMaxima",
            ]
        ]

    avaliacoes = avaliacoes.where(pd.notnull(avaliacoes), None)

    return avaliacoes, avaliacoes_ids


def atualizarAvaliacao(avaliacoes):

    print("Atualizando Avaliações...")

    conn = mdb.connect(**DB_CONFIG)

    cursor = conn.cursor()

    cursor.execute("SELECT id FROM tb_avaliacoes")
    ids_existentes = set(row[0] for row in cursor.fetchall())

    conn.commit()
    cursor.close()
    conn.close()

    avaliacoes_update = avaliacoes[avaliacoes["id"].isin(ids_existentes)].copy()
    avaliacoes_insert = avaliacoes[~avaliacoes["id"].isin(ids_existentes)].copy()
    ids_atuais = set(avaliacoes["id"])
    ids_para_deletar = [id_ for id_ in ids_existentes if id_ not in ids_atuais]
    avaliacoes_delete = pd.DataFrame({"id": ids_para_deletar})

    conn = mdb.connect(**DB_CONFIG)

    cursor = conn.cursor()

    for _, row in avaliacoes_update.iterrows():
        sql = """
        UPDATE tb_avaliacoes
        SET id = %s,
            loja_numero = %s,
            dtInicio = %s,
            dtFim = %s,
            dtCriacao = %s,
            dtPrazo = %s,
            modelo_id = %s,
            modelo_nome = %s,
            responsavel_id = %s,
            responsavel_nome = %s,
            pontuacaoAlcancada = %s,
            pontuacaoMaxima = %s
        WHERE id = %s
        """
        cursor.execute(
            sql,
            (
                row["id"],
                row["loja_numero"],
                row["dtInicio"],
                row["dtFim"],
                row["dtCriacao"],
                row["dtPrazo"],
                row["modelo.id"],
                row["modelo.nome"],
                row["responsavel.id"],
                row["responsavel.nome"],
                row["pontuacaoAlcancada"],
                row["pontuacaoMaxima"],
                row["id"],
            ),
        )

    print("Inserindo Avaliações...")

    for _, row in avaliacoes_insert.iterrows():
        sql = """
        INSERT INTO tb_avaliacoes (
            id, loja_numero, dtInicio, dtFim, dtCriacao, dtPrazo,
            modelo_id, modelo_nome, responsavel_id, responsavel_nome,
            pontuacaoAlcancada, pontuacaoMaxima) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(
            sql,
            (
                row["id"],
                row["loja_numero"],
                row["dtInicio"],
                row["dtFim"],
                row["dtCriacao"],
                row["dtPrazo"],
                row["modelo.id"],
                row["modelo.nome"],
                row["responsavel.id"],
                row["responsavel.nome"],
                row["pontuacaoAlcancada"],
                row["pontuacaoMaxima"],
            ),
        )

    if len(avaliacoes_delete) != 0:
        for _, row in avaliacoes_delete.iterrows():
            id_leads = int(row["id"])
            sql = "DELETE FROM tb_avaliacoes WHERE id = %s"
            cursor.execute(sql, (int(row["id"]),))

    conn.commit()
    cursor.close()
    conn.close()


############ RESPOSTAS #############
def buscarRespostas(avaliacoes_ids):

    print("Buscando Respostas...")

    def get_respostas(avaliacoes_ids):
        url = f"https://api.sults.com.br/api/v1/checklist/avaliacao/{avaliacoes_ids}/resposta"
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json().get("data", [])

            for resposta in data:
                resposta["avaliacao_id"] = avaliacoes_ids
            return data

        except Exception as e:
            print(f"Erro ao buscar avaliacao {avaliacoes_ids}: {e}")
            return []

    todas_respostas = []

    for avaliacao in avaliacoes_ids:
        resposta = get_respostas(avaliacao)
        todas_respostas.extend(resposta)

    df_respostas = pd.DataFrame(todas_respostas)
    respostas = df_respostas[
        [
            "id",
            "avaliacao_id",
            "questao",
            "resposta",
            "comentario",
            "pontuacaoAlcancada",
            "pontuacaoMaxima",
        ]
    ]

    respostas = respostas.where(pd.notnull(respostas), None)

    return respostas, df_respostas


def atualizarRespostas(respostas):

    print("Atualizando Respostas...")

    conn = mdb.connect(**DB_CONFIG)

    cursor = conn.cursor()

    cursor.execute("SELECT id FROM tb_respostas")
    ids_existentes = set(row[0] for row in cursor.fetchall())

    conn.commit()
    cursor.close()
    conn.close()

    respostas_update = respostas[respostas["id"].isin(ids_existentes)].copy()
    respostas_insert = respostas[~respostas["id"].isin(ids_existentes)].copy()
    ids_atuais = set(respostas["id"])
    ids_para_deletar = [id_ for id_ in ids_existentes if id_ not in ids_atuais]
    respostas_delete = pd.DataFrame({"id": ids_para_deletar})

    print("Inserindo Respostas...")

    conn = mdb.connect(**DB_CONFIG)

    cursor = conn.cursor()

    for _, row in respostas_update.iterrows():
        sql = """
        UPDATE tb_respostas
        SET id = %s,
            avaliacao_id = %s,
            questao = %s,
            resposta = %s,
            comentario = %s,
            pontuacaoAlcancada = %s,
            pontuacaoMaxima = %s
        WHERE id = %s
        """
        cursor.execute(
            sql,
            (
                row["id"],
                row["avaliacao_id"],
                row["questao"],
                row["resposta"],
                row["comentario"],
                row["pontuacaoAlcancada"],
                row["pontuacaoMaxima"],
                row["id"],
            ),
        )

    for _, row in respostas_insert.iterrows():
        sql = """
        INSERT INTO tb_respostas (
            id, avaliacao_id, questao, resposta, comentario, pontuacaoAlcancada, pontuacaoMaxima) 
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(
            sql,
            (
                row["id"],
                row["avaliacao_id"],
                row["questao"],
                row["resposta"],
                row["comentario"],
                row["pontuacaoAlcancada"],
                row["pontuacaoMaxima"],
            ),
        )

    if len(respostas_delete) != 0:
        for _, row in respostas_delete.iterrows():
            id_leads = int(row["id"])
            sql = "DELETE FROM tb_respostas WHERE id = %s"
            cursor.execute(sql, (int(row["id"]),))

    conn.commit()
    cursor.close()
    conn.close()


############ ANEXOS #############
def buscarAnexos(df_respostas):

    print("Buscando Anexos...")

    anexos_detalhados = []

    for _, row in df_respostas.iterrows():
        id_resposta = row["id"]
        anexos = row["anexo"]

        if isinstance(anexos, list) and anexos:
            for anexo in anexos:
                anexos_detalhados.append(
                    {
                        "id_resposta": id_resposta,
                        "anexo_id": anexo.get("id"),
                        "nome_arquivo": anexo.get("nome"),
                        "url": anexo.get("url"),
                        "dt_criacao": anexo.get("dtCriacao"),
                        "tamanho": anexo.get("tamanho"),
                    }
                )

    df_anexos = pd.DataFrame(anexos_detalhados)

    print("Tratando Anexos...")

    format_str = "%Y-%m-%dT%H:%M:%SZ"
    format_str = "%Y-%m-%dT%H:%M:%SZ"

    df_anexos["dt_criacao"] = pd.to_datetime(
        df_anexos["dt_criacao"], format=format_str, errors="coerce"
    )
    df_anexos["dt_criacao"] = df_anexos["dt_criacao"].dt.strftime("%Y-%m-%d %H:%M:%S")

    df_anexos = df_anexos.where(pd.notnull(df_anexos), None)

    return df_anexos


def atualizarAnexos(df_anexos):

    print("Atualizando Anexos...")

    conn = mdb.connect(**DB_CONFIG)

    cursor = conn.cursor()

    cursor.execute("SELECT anexo_id FROM tb_anexos")
    ids_existentes = set(row[0] for row in cursor.fetchall())

    conn.commit()
    cursor.close()
    conn.close()

    df_anexos_update = df_anexos[df_anexos["anexo_id"].isin(ids_existentes)].copy()
    df_anexos_insert = df_anexos[~df_anexos["anexo_id"].isin(ids_existentes)].copy()
    ids_atuais = set(df_anexos["anexo_id"])
    ids_para_deletar = [id_ for id_ in ids_existentes if id_ not in ids_atuais]
    df_anexos_delete = pd.DataFrame({"anexo_id": ids_para_deletar})

    print("Inserindo Anexos...")

    conn = mdb.connect(**DB_CONFIG)

    cursor = conn.cursor()

    for _, row in df_anexos_update.iterrows():
        sql = """
        UPDATE tb_anexos
        SET anexo_id = %s,
            id_resposta = %s,
            nome_arquivo = %s,
            url = %s,
            dt_criacao = %s
        WHERE anexo_id = %s
        """
        cursor.execute(
            sql,
            (
                row["anexo_id"],
                row["id_resposta"],
                row["nome_arquivo"],
                row["url"],
                row["dt_criacao"],
                row["anexo_id"],
            ),
        )

    for _, row in df_anexos_insert.iterrows():
        sql = """
        INSERT INTO tb_anexos (
            anexo_id, id_resposta, nome_arquivo, url, dt_criacao) 
        VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(
            sql,
            (
                row["anexo_id"],
                row["id_resposta"],
                row["nome_arquivo"],
                row["url"],
                row["dt_criacao"],
            ),
        )

    if len(df_anexos_delete) != 0:
        for _, row in df_anexos_delete.iterrows():
            id = int(row["id"])
            sql = "DELETE FROM tb_anexos WHERE id = %s"
            cursor.execute(sql, (int(row["id"]),))

    conn.commit()
    cursor.close()
    conn.close()


############ TELEGRAM ##############
def mandarMSG(message):
    token = "8012309317:AAFhKzdAinj3P7PL9-YyNvaNH7LPJYB5M4U"
    chat_id = "952443530"
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    params = {"chat_id": chat_id, "text": message}
    response = requests.get(url, params=params)


############ PRINCIPAL #############
def main():
    try:
        ############ PROJETOS #############
        print("############ PROJETOS #############")
        df = buscarProjetos()
        Projetos = tratarProjetos(df)
        projetos_update, projetos_insert, projetos_delete = pegarIDs(
            Projetos, "tb_projetos"
        )
        atualizarProjetos(projetos_update)
        inserirProjetos(projetos_insert)
        if len(projetos_delete) != 0:
            apagar(projetos_delete, "tb_projetos")

        ############ TAREFAS #############
        print("############ TAREFAS #############")
        projetos_ids = Projetos["id"].astype(str).tolist()
        df = buscarTarefas(projetos_ids)
        Tarefas = tratarTarefas(df)
        tarefas_update, tarefas_insert, tarefas_delete = pegarIDs(Tarefas, "tb_tarefas")
        atualizarTarefas(tarefas_update)
        inserirTarefas(tarefas_insert)
        if len(tarefas_delete) != 0:
            apagar(tarefas_delete, "tb_tarefas")

        ############ LEADS #############
        print("############ LEADS #############")
        df = buscarLeads()
        Leads = tratarLeads(df)
        leads_update, leads_insert, leads_delete = pegarIDs(Leads, "tb_leads")
        atualizarLeads(leads_update)
        inserirLeads(leads_insert)
        if len(leads_delete) != 0:
            apagar(leads_delete, "tb_leads")

        ############ TIMELINES #############
        print("############ TIMELINES #############")
        leads_ids = Leads["id"].astype(str).tolist()
        df = buscarTimelines(leads_ids)
        Timelines = tratarTimelines(df)
        timelines_update, timeslines_insert, timelines_delete = pegarIDs(
            Timelines, "tb_timelines"
        )
        atualizarTimelines(timelines_update)
        inserirTimelines(timeslines_insert)
        if len(timelines_delete) != 0:
            apagar(timelines_delete, "tb_timelines")

        ############ AVALIAÇÕES #############
        print("############ AVALIAÇÕES #############")
        avaliacoes_df, avaliacoes_ids = buscarAvaliacao()
        atualizarAvaliacao(avaliacoes_df)

        ############ RESPOSTAS #############
        print("############ RESPOSTAS #############")
        respostas, df_respostas = buscarRespostas(avaliacoes_ids)
        atualizarRespostas(respostas)

        ############ ANEXOS #############
        print("############ ANEXOS #############")
        anexos = buscarRespostas(df_respostas)
        atualizarRespostas(anexos)

        print("Processo Concluído!")
        mandarMSG(f"{agora} - ✅ Atualização do SULTS realizada com sucesso!")

    except Exception as e:
        print(e)
        mandarMSG(f"{agora} - ❌ Erro ao atualizar o SULTS = {e}")


if __name__ == "__main__":
    main()
