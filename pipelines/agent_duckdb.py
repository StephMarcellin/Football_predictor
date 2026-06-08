# pipelines/agent_duckdb.py
from __future__ import annotations
from dotenv import load_dotenv
load_dotenv()

import os
import sys
import yaml
from pathlib import Path
from typing import TypedDict, Optional

import duckdb
from langchain_groq import ChatGroq
from langchain_core.prompts import ChatPromptTemplate
from langgraph.graph import StateGraph, START, END

# 1. Définition de l'état partagé
class AgentState(TypedDict):
    question: str                    # question posée par l'utilisateur
    sql_query: Optional[str]         # SQL généré par le LLM
    sql_result: Optional[str]        # résultats bruts retournés par DuckDB
    error: Optional[str]             # message d'erreur si execute_sql échoue
    final_answer: Optional[str]      # réponse mise en forme par le LLM

# 2. Connection duckdb
# ── Chemins ───────────────────────────────────────────────────────────────────
ROOT_DIR = Path(__file__).resolve().parent.parent

with open(ROOT_DIR / "config.yaml", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

DB_PATH    = ROOT_DIR / CFG["paths"]["duckdb"]


# ── Connexion DuckDB (lecture seule) ─────────────────────────────────────────
def get_db_connection() -> duckdb.DuckDBPyConnection:
    """Ouvre une connexion read-only sur football.duckdb."""
    return duckdb.connect(str(DB_PATH), read_only=True)

# ── Schéma injecté dans le prompt ─────────────────────────────────────────────
def load_schema() -> str:
    """
    Retourne la liste des colonnes de gold.features_final
    sous forme de texte, à injecter dans le system prompt.
    """
    try:
        con = get_db_connection()
        rows = con.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'gold'
              AND table_name   = 'features_final'
            ORDER BY ordinal_position
        """).fetchall()
        con.close()
        return "\n".join(f"  - {col} ({dtype})" for col, dtype in rows)
    except Exception as e:
        return f"Schéma indisponible : {e}"
    
# 3. Le modèle
# ── LLM ───────────────────────────────────────────────────────────────────────
llm = ChatGroq(
    model="llama-3.3-70b-versatile",
    api_key=os.getenv("GROQ_API_KEY"),
    temperature=0,   # génération SQL : on veut du déterministe, pas de créativité
)

# ── Chargement du schéma au démarrage ─────────────────────────────────────────
SCHEMA = load_schema()


# 4. Les nœuds du graphe
# ── Nœud 1 : generate_sql ─────────────────────────────────────────────────────
def generate_sql(state: AgentState) -> dict:
    """
    Reçoit la question de l'utilisateur, retourne une requête SQL
    prête à être exécutée sur gold.features_final.
    """
    prompt = ChatPromptTemplate.from_messages([
       ("system", """Tu génères des requêtes SQL pour DuckDB sur la table gold.features_final.

Colonnes disponibles dans gold.features_final :
{schema}

Tables référentielles disponibles :
- referentiel.team_mapping (alias VARCHAR, club_name VARCHAR) — mapping des noms d'équipes
- referentiel.competition_mapping (alias VARCHAR, competition_name VARCHAR) — mapping des compétitions

Quand l'utilisateur mentionne une équipe ou une compétition, utilise TOUJOURS une sous-requête pour résoudre le nom canonique :
SELECT ... FROM gold.features_final
WHERE team = (SELECT club_name FROM referentiel.team_mapping WHERE alias ILIKE ... LIMIT 1)

Exemple complet :
SELECT COUNT(*) FROM gold.features_final
WHERE team = (SELECT club_name FROM referentiel.team_mapping WHERE alias ILIKE '%arsenal%' LIMIT 1)

Règles absolues :
- Retourne UNIQUEMENT la requête SQL, sans explication, sans balises markdown
- Utilise toujours le préfixe gold.features_final
- Limite les résultats à 50 lignes maximum avec LIMIT 50
- N'utilise que les colonnes listées ci-dessus"""),
            ("human", "{question}"),
        ])

    chain = prompt | llm

    response = chain.invoke({
        "schema": SCHEMA,
        "question": state["question"],
    })

    return {"sql_query": response.content.strip()}

# ── Nœud 2 : execute_sql ──────────────────────────────────────────────────────
def execute_sql(state: AgentState) -> dict:
    """
    Exécute le SQL généré sur DuckDB.
    Retourne les résultats sous forme de texte, ou une erreur.
    """
    try:
        con = get_db_connection()
        rows = con.execute(state["sql_query"]).fetchall()
        cols = [desc[0] for desc in con.description]
        con.close()

        if not rows:
            return {"sql_result": "Aucun résultat retourné.", "error": None}

        # Formatage en tableau texte lisible par le LLM
        header = " | ".join(cols)
        lines  = [" | ".join(str(v) for v in row) for row in rows]
        result = "\n".join([header, "-" * len(header)] + lines)

        return {"sql_result": result, "error": None}

    except Exception as e:
        return {"sql_result": None, "error": str(e)}
    

# ── Nœud 3 : format_response ──────────────────────────────────────────────────
def format_response(state: AgentState) -> dict:
    """
    Reçoit les résultats SQL et formule une réponse lisible.
    Gère aussi le cas d'erreur SQL.
    """
    prompt = ChatPromptTemplate.from_messages([
        ("system", """Tu reçois les résultats d'une requête SQL sur une base de données football.
Formule une réponse claire et concise en français à la question posée. Sois consis, une ou deux phrases suffisent.
Si une erreur SQL est présente, explique ce qui s'est passé simplement."""),
        ("human", """Question : {question}

Requête SQL exécutée : {sql_query}

Résultats : {sql_result}

Erreur éventuelle : {error}"""),
    ])

    chain = prompt | llm

    response = chain.invoke({
        "question":   state["question"],
        "sql_query":  state["sql_query"],
        "sql_result": state["sql_result"] or "Aucun résultat",
        "error":      state["error"] or "Aucune",
    })

    return {"final_answer": response.content.strip()}

# 5. Assemblage du graphe ───────────────────────────────────────────────────────
def build_graph():
    """
    Assemble et compile le graphe LangGraph.
    Retourne un graphe prêt à être invoqué.
    """
    graph = StateGraph(AgentState)

    # Ajout des nœuds
    graph.add_node("generate_sql",    generate_sql)
    graph.add_node("execute_sql",     execute_sql)
    graph.add_node("format_response", format_response)

    # Définition du flux
    graph.add_edge(START,             "generate_sql")
    graph.add_edge("generate_sql",    "execute_sql")
    graph.add_edge("execute_sql",     "format_response")
    graph.add_edge("format_response", END)

    return graph.compile()

# ── Point d'entrée ────────────────────────────────────────────────────────────
def main():
    """
    Boucle interactive CLI.
    Pose une question, l'agent interroge DuckDB et répond.
    """
    print("Agent DuckDB — Projet 3-Étoiles")
    print("Tape 'exit' pour quitter\n")

    agent = build_graph()

    while True:
        question = input("Question : ").strip()

        if question.lower() == "exit":
            break

        if not question:
            continue

        result = agent.invoke({"question": question})

        print(f"\nSQL généré    : {result['sql_query']}")
        print(f"Réponse       : {result['final_answer']}\n")


if __name__ == "__main__":
    main()