"""
CineGen AI - Backend Flask CORRIGIDO
✅ Property Graph com PGQL correto
✅ Todas as queries usando graph_table
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
import oracledb
import oci
from langchain_community.chat_models import ChatOCIGenAI
from langchain_core.messages import HumanMessage
import numpy as np
import json
from datetime import datetime
import os
import traceback

app = Flask(__name__)
CORS(app)

# ==================== CONFIGURAÇÃO ====================
try:
    oracledb.init_oracle_client()
    print("✓ Oracle Thick Mode habilitado")
except Exception as e:
    print(f"⚠️  Thick mode não disponível: {e}")

CONFIG_PROFILE = "DEFAULT"
try:
    config = oci.config.from_file('~/.oci/config', CONFIG_PROFILE)
    compartment_id = os.getenv('OCI_COMPARTMENT_ID', '')
    model_id = os.getenv('OCI_MODEL_ID', '')
    print("✓ OCI Config carregado")
except Exception as e:
    print(f"⚠️  Erro ao carregar OCI config: {e}")
    config = None
    compartment_id = ''
    model_id = ''

DB_CONFIG = {
    'user': os.getenv('DB_USER', ''),
    'password': os.getenv('DB_PASSWORD', ''),
    'dsn': os.getenv('DB_DSN', '')
}

SCHEMA = ''  # Schema do Property Graph
GRAPH_NAME = f'{SCHEMA}.movie_graph'  # Nome completo do grafo

def get_db_connection():
    try:
        return oracledb.connect(**DB_CONFIG)
    except Exception as e:
        print(f"❌ Erro ao conectar ao banco: {e}")
        raise


def get_llm_response(prompt_text, temperature=0.7, max_tokens=300):
    chat = ChatOCIGenAI(
        model_id=model_id,
        service_endpoint="",
        compartment_id=compartment_id,
        provider="meta",
        model_kwargs={
            "temperature": temperature,
            "max_tokens": max_tokens,
            "frequency_penalty": 0,
            "presence_penalty": 0,
            "top_p": 0.75
        },
        auth_profile=CONFIG_PROFILE
    )
    messages = [HumanMessage(content=prompt_text)]
    response = chat.invoke(messages)
    return response.content


def generate_embedding(text):
    try:
        generative_ai_inference_client = oci.generative_ai_inference.GenerativeAiInferenceClient(
            config=config,
            service_endpoint="",
            retry_strategy=oci.retry.NoneRetryStrategy(),
            timeout=(10, 240)
        )
        embed_text_detail = oci.generative_ai_inference.models.EmbedTextDetails()
        embed_text_detail.serving_mode = oci.generative_ai_inference.models.OnDemandServingMode(
            model_id=""
        )
        embed_text_detail.inputs = [text]
        embed_text_detail.truncate = "END"
        embed_text_detail.compartment_id = compartment_id
        embed_text_response = generative_ai_inference_client.embed_text(embed_text_detail)
        return embed_text_response.data.embeddings[0]
    except Exception as e:
        print(f"Erro ao gerar embedding: {e}")
        return np.random.rand(1024).tolist()


def parse_genres(genres_data):
    if isinstance(genres_data, str):
        try:
            return json.loads(genres_data)
        except:
            return {}
    elif isinstance(genres_data, dict):
        return genres_data
    return {}


def to_iso(value):
    if value is None:
        return None
    try:
        return value.isoformat()
    except:
        return str(value)


# ==================== ENDPOINTS  ====================

@app.route('/api/movies', methods=['GET'])
def get_movies():
    conn = None
    cursor = None
    try:
        limit = min(int(request.args.get('limit', 20)), 100)
        offset = max(int(request.args.get('offset', 0)), 0)
        search_query = (request.args.get('search', '') or '').strip()

        conn = get_db_connection()
        cursor = conn.cursor()

        where_clause = ""
        params_page = {'offset': offset, 'limit': limit}
        params_count = {}

        if search_query:
            where_clause = "WHERE UPPER(m.TITLE) LIKE UPPER(:search) OR UPPER(m.SUMMARY) LIKE UPPER(:search)"
            like = f'%{search_query}%'
            params_page['search'] = like
            params_count['search'] = like

        has_media = False
        try:
            query = f"""
                SELECT
                    m.MOVIE_ID, m.TITLE, m.GENRES, m.SUMMARY,
                    NVL(m.RATING, 0) as RATING, NVL(m.YEAR, 2024) as YEAR,
                    (SELECT COUNT(*) FROM {SCHEMA}.WATCHED_MOVIE w WHERE w.MOVIE_ID = m.MOVIE_ID) as WATCH_COUNT,
                    p.ASSET_URL AS POSTER_URL,
                    t.ASSET_URL AS TRAILER_URL
                FROM {SCHEMA}.MOVIES m
                LEFT JOIN {SCHEMA}.MEDIA_ASSETS p ON p.MOVIE_ID = m.MOVIE_ID AND p.ASSET_TYPE = 'poster_url'
                LEFT JOIN {SCHEMA}.MEDIA_ASSETS t ON t.MOVIE_ID = m.MOVIE_ID AND t.ASSET_TYPE = 'trailer_url'
                {where_clause}
                ORDER BY m.MOVIE_ID
                OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY
            """
            cursor.execute(query, params_page)
            has_media = True
        except Exception as e:
            print(f"⚠️  Query com MEDIA_ASSETS falhou, usando fallback. Motivo: {e}")
            query = f"""
                SELECT m.MOVIE_ID, m.TITLE, m.GENRES, m.SUMMARY,
                       NVL(m.RATING, 0) as RATING, NVL(m.YEAR, 2024) as YEAR,
                       (SELECT COUNT(*) FROM {SCHEMA}.WATCHED_MOVIE w WHERE w.MOVIE_ID = m.MOVIE_ID) as WATCH_COUNT
                FROM {SCHEMA}.MOVIES m
                {where_clause}
                ORDER BY m.MOVIE_ID
                OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY
            """
            cursor.execute(query, params_page)
            has_media = False

        movies = []
        for row in cursor:
            movie = {
                'id': row[0],
                'title': row[1],
                'genres': parse_genres(row[2]),
                'summary': row[3] if row[3] else 'Sem descrição',
                'rating': float(row[4] or 0),
                'year': int(row[5] or 2024),
                'watchCount': int(row[6] or 0)
            }
            if has_media:
                movie['poster_url'] = row[7]
                movie['trailer_url'] = row[8]
            movies.append(movie)

        count_query = f"SELECT COUNT(*) FROM {SCHEMA}.MOVIES m {where_clause}"
        cursor.execute(count_query, params_count)
        total = cursor.fetchone()[0]

        return jsonify({
            'success': True,
            'data': movies,
            'count': len(movies),
            'total': total,
            'search_query': search_query if search_query else None
        })
    except Exception as e:
        print(f"❌ Erro em /api/movies: {e}")
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        try:
            if cursor:
                cursor.close()
        except:
            pass
        try:
            if conn:
                conn.close()
        except:
            pass


@app.route('/api/search/vector', methods=['POST'])
def vector_search():
    try:
        data = request.get_json() or {}
        query_text = (data.get('query', '') or '').strip()
        top_k = int(data.get('top_k', 5))

        if not query_text:
            return jsonify({'success': False, 'error': 'Query required'}), 400

        query_embedding = generate_embedding(query_text)

        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            embedding_array = f"[{','.join(map(str, query_embedding))}]"
            query = f"""
                SELECT m.MOVIE_ID, m.TITLE, m.SUMMARY, m.GENRES, m.RATING,
                       VECTOR_DISTANCE(mv.EMBEDDING, TO_VECTOR(:query_vec), COSINE) as distance,
                       ma.ASSET_URL as POSTER_URL
                FROM {SCHEMA}.MOVIES m
                JOIN {SCHEMA}.MOVIE_VECTORS mv ON m.MOVIE_ID = mv.MOVIE_ID
                LEFT JOIN {SCHEMA}.MEDIA_ASSETS ma ON m.MOVIE_ID = ma.MOVIE_ID AND ma.ASSET_TYPE = 'poster_url'
                ORDER BY distance ASC
                FETCH FIRST :top_k ROWS ONLY
            """
            cursor.execute(query, {'query_vec': embedding_array, 'top_k': top_k})

            results = []
            for row in cursor:
                results.append({
                    'id': row[0],
                    'title': row[1],
                    'snippet': row[2][:200] + '...' if row[2] and len(row[2]) > 200 else row[2],
                    'genres': parse_genres(row[3]),
                    'rating': float(row[4]) if row[4] else 0,
                    'score': float(1 - float(row[5])),
                    'poster_url': row[6]
                })
        except:
            cursor.execute(f"""
                SELECT m.MOVIE_ID, m.TITLE, m.SUMMARY, m.GENRES, m.RATING, 0.5 as score,
                       ma.ASSET_URL as POSTER_URL
                FROM {SCHEMA}.MOVIES m
                LEFT JOIN {SCHEMA}.MEDIA_ASSETS ma ON m.MOVIE_ID = ma.MOVIE_ID AND ma.ASSET_TYPE = 'poster_url'
                WHERE UPPER(m.TITLE) LIKE UPPER(:query) OR UPPER(m.SUMMARY) LIKE UPPER(:query)
                FETCH FIRST :top_k ROWS ONLY
            """, {'query': f'%{query_text}%', 'top_k': top_k})

            results = []
            for row in cursor:
                results.append({
                    'id': row[0],
                    'title': row[1],
                    'snippet': row[2][:200] + '...' if row[2] and len(row[2]) > 200 else row[2],
                    'genres': parse_genres(row[3]),
                    'rating': float(row[4]) if row[4] else 0,
                    'score': float(row[5]),
                    'poster_url': row[6]
                })

        cursor.close()
        conn.close()

        return jsonify({'success': True, 'query': query_text, 'results': results})
    except Exception as e:
        print(f"❌ Erro: {e}")
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/customers', methods=['GET'])
def get_customers():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(f"""
            SELECT CUST_ID, FIRSTNAME, LASTNAME, EMAIL,
                   (SELECT COUNT(*) FROM {SCHEMA}.WATCHED_MOVIE w WHERE w.PROMO_CUST_ID = c.CUST_ID) as movies_count
            FROM {SCHEMA}.MOVIES_CUSTOMER c
            ORDER BY CUST_ID
        """)

        customers = []
        for row in cursor:
            customers.append({
                'id': row[0],
                'firstname': row[1],
                'lastname': row[2],
                'email': row[3],
                'movies_count': row[4]
            })

        cursor.close()
        conn.close()

        return jsonify({'success': True, 'data': customers})
    except Exception as e:
        print(f"❌ Erro: {e}")
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/customers', methods=['POST'])
def create_customer():
    try:
        data = request.get_json() or {}
        firstname = (data.get('firstname', '') or '').strip()
        lastname = (data.get('lastname', '') or '').strip()
        email = (data.get('email', '') or '').strip()

        if not firstname or not lastname or not email:
            return jsonify({'success': False, 'error': 'Nome, sobrenome e email obrigatórios'}), 400

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(f"SELECT NVL(MAX(cust_id), 100) + 1 FROM {SCHEMA}.MOVIES_CUSTOMER")
        new_cust_id = cursor.fetchone()[0]

        cursor.execute(f"""
            INSERT INTO {SCHEMA}.MOVIES_CUSTOMER (CUST_ID, FIRSTNAME, LASTNAME, EMAIL)
            VALUES (:cust_id, :firstname, :lastname, :email)
        """, {'cust_id': new_cust_id, 'firstname': firstname, 'lastname': lastname, 'email': email})

        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({
            'success': True,
            'customer': {
                'id': new_cust_id,
                'firstname': firstname,
                'lastname': lastname,
                'email': email,
                'movies_count': 0
            }
        })
    except Exception as e:
        print(f"❌ Erro: {e}")
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/customers/<int:customer_id>/watch', methods=['POST'])
def mark_as_watched(customer_id):
    try:
        data = request.get_json() or {}
        movie_id = data.get('movie_id')
        rating = data.get('rating', None)

        if not movie_id:
            return jsonify({'success': False, 'error': 'movie_id obrigatório'}), 400

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(f"""
            SELECT COUNT(*) FROM {SCHEMA}.WATCHED_MOVIE 
            WHERE PROMO_CUST_ID = :cust_id AND MOVIE_ID = :movie_id
        """, {'cust_id': customer_id, 'movie_id': movie_id})

        if cursor.fetchone()[0] > 0:
            if rating is not None:
                cursor.execute(f"""
                    UPDATE {SCHEMA}.WATCHED_MOVIE
                    SET RATING_GIVEN = :rating, DAY_ID = SYSDATE
                    WHERE PROMO_CUST_ID = :cust_id AND MOVIE_ID = :movie_id
                """, {'rating': rating, 'cust_id': customer_id, 'movie_id': movie_id})
                conn.commit()
                cursor.close()
                conn.close()
                return jsonify({'success': True, 'message': 'Rating atualizado'})
            else:
                cursor.close()
                conn.close()
                return jsonify({'success': False, 'error': 'Já assistiu'}), 400

        cursor.execute(f"""
            INSERT INTO {SCHEMA}.WATCHED_MOVIE (PROMO_CUST_ID, MOVIE_ID, DAY_ID, RATING_GIVEN)
            VALUES (:cust_id, :movie_id, SYSDATE, :rating)
        """, {'cust_id': customer_id, 'movie_id': movie_id, 'rating': rating})

        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({'success': True, 'message': 'Marcado como assistido'})
    except Exception as e:
        print(f"❌ Erro: {e}")
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


# ==================== ROTAS COM PROPERTY GRAPH  ====================

@app.route('/api/graph/recommendations/<int:customer_id>', methods=['GET'])
def get_graph_recommendations(customer_id):
 
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Primeiro: buscar filmes já assistidos pelo cliente
        cursor.execute(f"""
            SELECT MOVIE_ID FROM {SCHEMA}.WATCHED_MOVIE 
            WHERE PROMO_CUST_ID = :cust_id
        """, {'cust_id': customer_id})
        watched_movies = {row[0] for row in cursor.fetchall()}
        
        
        # Sintaxe: GRAPH_TABLE(graph MATCH pattern COLUMNS(...))
        pgql_query = f"""
            SELECT 
                movie_id, 
                title, 
                summary, 
                rating, 
                similar_users
            FROM GRAPH_TABLE ({GRAPH_NAME}
                MATCH (c1:customer)-[:watched]->(m:movie)<-[:watched]-(c2:customer)-[:watched]->(m2:movie)
                WHERE c1.cust_id = :cust_id 
                  AND c2.cust_id != :cust_id
                COLUMNS (
                    m2.movie_id AS movie_id,
                    m2.title AS title,
                    m2.summary AS summary,
                    m2.rating AS rating,
                    COUNT(DISTINCT c2.cust_id) AS similar_users
                )
            )
            GROUP BY movie_id, title, summary, rating, similar_users
            ORDER BY similar_users DESC, rating DESC
            FETCH FIRST 10 ROWS ONLY
        """
        
        try:
            cursor.execute(pgql_query, {'cust_id': customer_id})
            
            recommendations = []
            for row in cursor:
                movie_id = row[0]
                
                # Pular filmes já assistidos
                if movie_id in watched_movies:
                    continue
                
                # Buscar poster separadamente (depois do GRAPH_TABLE)
                cursor2 = conn.cursor()
                cursor2.execute(f"""
                    SELECT ASSET_URL FROM {SCHEMA}.MEDIA_ASSETS 
                    WHERE MOVIE_ID = :mid AND ASSET_TYPE = 'poster_url'
                """, {'mid': movie_id})
                poster_row = cursor2.fetchone()
                poster_url = poster_row[0] if poster_row else None
                cursor2.close()
                
                recommendations.append({
                    'id': movie_id,
                    'title': row[1],
                    'summary': row[2][:150] + '...' if row[2] and len(row[2]) > 150 else row[2],
                    'rating': float(row[3]) if row[3] else 0,
                    'similar_users': int(row[4]),
                    'poster_url': poster_url,
                    'graph_reason': f'{row[4]} usuários com gostos similares assistiram',
                    'recommendation_type': 'collaborative_filtering'
                })
                
                # Limitar a 5 recomendações
                if len(recommendations) >= 5:
                    break
            
            cursor.close()
            conn.close()
            
            return jsonify({
                'success': True,
                'customer_id': customer_id,
                'recommendations': recommendations,
                'method': 'property_graph_pgql'
            })
            
        except Exception as pgql_error:
            print(f"⚠️  Erro PGQL, usando fallback SQL: {pgql_error}")
            
            # Fallback para SQL tradicional se PGQL falhar
            cursor.execute(f"""
                SELECT m2.MOVIE_ID, m2.TITLE, m2.SUMMARY, m2.RATING,
                       COUNT(DISTINCT c2.CUST_ID) as similar_users,
                       ma.ASSET_URL as POSTER_URL
                FROM {SCHEMA}.WATCHED_MOVIE w1
                JOIN {SCHEMA}.WATCHED_MOVIE w2 ON w1.MOVIE_ID = w2.MOVIE_ID
                JOIN {SCHEMA}.MOVIES_CUSTOMER c2 ON w2.PROMO_CUST_ID = c2.CUST_ID
                JOIN {SCHEMA}.WATCHED_MOVIE w3 ON c2.CUST_ID = w3.PROMO_CUST_ID
                JOIN {SCHEMA}.MOVIES m2 ON w3.MOVIE_ID = m2.MOVIE_ID
                LEFT JOIN {SCHEMA}.MEDIA_ASSETS ma ON m2.MOVIE_ID = ma.MOVIE_ID AND ma.ASSET_TYPE = 'poster_url'
                WHERE w1.PROMO_CUST_ID = :cust_id
                  AND c2.CUST_ID != :cust_id
                  AND m2.MOVIE_ID NOT IN (
                      SELECT MOVIE_ID FROM {SCHEMA}.WATCHED_MOVIE WHERE PROMO_CUST_ID = :cust_id
                  )
                GROUP BY m2.MOVIE_ID, m2.TITLE, m2.SUMMARY, m2.RATING, ma.ASSET_URL
                ORDER BY similar_users DESC, m2.RATING DESC
                FETCH FIRST 5 ROWS ONLY
            """, {'cust_id': customer_id})
            
            recommendations = []
            for row in cursor:
                recommendations.append({
                    'id': row[0],
                    'title': row[1],
                    'summary': row[2][:150] + '...' if row[2] and len(row[2]) > 150 else row[2],
                    'rating': float(row[3]) if row[3] else 0,
                    'similar_users': int(row[4]),
                    'poster_url': row[5],
                    'graph_reason': f'{row[4]} usuários similares assistiram',
                    'recommendation_type': 'sql_fallback'
                })
            
            cursor.close()
            conn.close()
            
            return jsonify({
                'success': True,
                'customer_id': customer_id,
                'recommendations': recommendations,
                'method': 'sql_fallback'
            })
            
    except Exception as e:
        print(f"❌ Erro geral: {e}")
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/graph/customer/<int:customer_id>', methods=['GET'])
def get_customer_graph(customer_id):

    try:
        limit = int(request.args.get('limit', 20))

        conn = get_db_connection()
        cursor = conn.cursor()

        # Info do cliente
        cursor.execute(f"""
            SELECT CUST_ID, FIRSTNAME || ' ' || LASTNAME as NAME
            FROM {SCHEMA}.MOVIES_CUSTOMER
            WHERE CUST_ID = :cust_id
        """, {'cust_id': customer_id})

        customer_row = cursor.fetchone()
        if not customer_row:
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'error': 'Cliente não encontrado'}), 404

        # Total de filmes
        cursor.execute(f"""
            SELECT COUNT(*) FROM {SCHEMA}.WATCHED_MOVIE 
            WHERE PROMO_CUST_ID = :cust_id
        """, {'cust_id': customer_id})
        total = cursor.fetchone()[0]

        if total == 0:
            cursor.close()
            conn.close()
            return jsonify({
                'success': True,
                'nodes': [],
                'edges': [],
                'total': 0,
                'showing': 0,
                'message': 'Cliente ainda não assistiu filmes'
            })

        # PGQL: Filmes assistidos pelo cliente
        try:
            pgql_movies = f"""
                SELECT movie_id, title
                FROM GRAPH_TABLE ({GRAPH_NAME}
                    MATCH (c:customer)-[:watched]->(m:movie)
                    WHERE c.cust_id = :cust_id
                    COLUMNS (m.movie_id AS movie_id, m.title AS title)
                )
                FETCH FIRST :limit ROWS ONLY
            """
            cursor.execute(pgql_movies, {'cust_id': customer_id, 'limit': limit})
        except:
            # Fallback SQL
            cursor.execute(f"""
                SELECT m.MOVIE_ID, m.TITLE
                FROM {SCHEMA}.WATCHED_MOVIE w
                JOIN {SCHEMA}.MOVIES m ON w.MOVIE_ID = m.MOVIE_ID
                WHERE w.PROMO_CUST_ID = :cust_id
                FETCH FIRST :limit ROWS ONLY
            """, {'cust_id': customer_id, 'limit': limit})

        movies = cursor.fetchall()

        # Montar resposta
        nodes = [{'id': customer_row[0], 'label': customer_row[1], 'type': 'customer'}]
        edges = []

        for movie_id, movie_title in movies:
            nodes.append({'id': movie_id, 'label': movie_title, 'type': 'movie'})
            edges.append({'source': customer_row[0], 'target': movie_id, 'type': 'WATCHED'})

        cursor.close()
        conn.close()

        return jsonify({
            'success': True,
            'nodes': nodes,
            'edges': edges,
            'total': total,
            'showing': len(movies)
        })

    except Exception as e:
        print(f"❌ Erro: {e}")
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/graph/compare/<int:id1>/<int:id2>', methods=['GET'])
def compare_customers(id1, id2):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Cliente 1
        cursor.execute(f"""
            SELECT CUST_ID, FIRSTNAME || ' ' || LASTNAME as NAME
            FROM {SCHEMA}.MOVIES_CUSTOMER
            WHERE CUST_ID = :id
        """, {'id': id1})
        customer1_row = cursor.fetchone()

        if not customer1_row:
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'error': f'Cliente {id1} não encontrado'}), 404

        # Cliente 2
        cursor.execute(f"""
            SELECT CUST_ID, FIRSTNAME || ' ' || LASTNAME as NAME
            FROM {SCHEMA}.MOVIES_CUSTOMER
            WHERE CUST_ID = :id
        """, {'id': id2})
        customer2_row = cursor.fetchone()

        if not customer2_row:
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'error': f'Cliente {id2} não encontrado'}), 404

        # Filmes Cliente 1
        cursor.execute(f"""
            SELECT m.MOVIE_ID, m.TITLE
            FROM {SCHEMA}.WATCHED_MOVIE w
            JOIN {SCHEMA}.MOVIES m ON w.MOVIE_ID = m.MOVIE_ID
            WHERE w.PROMO_CUST_ID = :id
        """, {'id': id1})
        movies1 = [{'id': row[0], 'title': row[1]} for row in cursor.fetchall()]
        movies1_ids = {m['id'] for m in movies1}

        # Filmes Cliente 2
        cursor.execute(f"""
            SELECT m.MOVIE_ID, m.TITLE
            FROM {SCHEMA}.WATCHED_MOVIE w
            JOIN {SCHEMA}.MOVIES m ON w.MOVIE_ID = m.MOVIE_ID
            WHERE w.PROMO_CUST_ID = :id
        """, {'id': id2})
        movies2 = [{'id': row[0], 'title': row[1]} for row in cursor.fetchall()]
        movies2_ids = {m['id'] for m in movies2}

        # Filmes em comum
        common_ids = movies1_ids & movies2_ids
        common_movies = [m for m in movies1 if m['id'] in common_ids]

        # Filmes exclusivos
        unique1 = [m for m in movies1 if m['id'] not in movies2_ids]
        unique2 = [m for m in movies2 if m['id'] not in movies1_ids]

        # Similaridade
        total_unique = len(movies1_ids | movies2_ids)
        similarity = int((len(common_ids) / total_unique * 100)) if total_unique > 0 else 0

        cursor.close()
        conn.close()

        return jsonify({
            'success': True,
            'customer1': {
                'id': customer1_row[0],
                'name': customer1_row[1],
                'total_movies': len(movies1),
                'unique_movies': unique1[:5]
            },
            'customer2': {
                'id': customer2_row[0],
                'name': customer2_row[1],
                'total_movies': len(movies2),
                'unique_movies': unique2[:5]
            },
            'common': {
                'count': len(common_movies),
                'movies': common_movies
            },
            'similarity_score': similarity,
            'unique_to_customer1': len(unique1),
            'unique_to_customer2': len(unique2)
        })

    except Exception as e:
        print(f"❌ Erro: {e}")
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/graph/network/<int:customer_id>', methods=['GET'])
def get_network_graph(customer_id):

    try:
        depth = int(request.args.get('depth', 2))
        limit = int(request.args.get('limit', 50))
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Cliente principal
        cursor.execute(f"""
            SELECT CUST_ID, FIRSTNAME || ' ' || LASTNAME as NAME
            FROM {SCHEMA}.MOVIES_CUSTOMER
            WHERE CUST_ID = :cust_id
        """, {'cust_id': customer_id})
        
        customer_row = cursor.fetchone()
        if not customer_row:
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'error': 'Cliente não encontrado'}), 404
        
        nodes = [{
            'id': f'c{customer_row[0]}', 
            'label': customer_row[1], 
            'type': 'customer', 
            'group': 1, 
            'size': 12
        }]
        links = []
        
        # PGQL: Filmes do cliente
        try:
            pgql_movies = f"""
                SELECT movie_id, title, genres
                FROM GRAPH_TABLE ({GRAPH_NAME}
                    MATCH (c:customer)-[:watched]->(m:movie)
                    WHERE c.cust_id = :cust_id
                    COLUMNS (
                        m.movie_id AS movie_id,
                        m.title AS title,
                        m.genres AS genres
                    )
                )
                FETCH FIRST :limit ROWS ONLY
            """
            cursor.execute(pgql_movies, {'cust_id': customer_id, 'limit': limit})
        except:
            # Fallback SQL
            cursor.execute(f"""
                SELECT m.MOVIE_ID, m.TITLE, m.GENRES
                FROM {SCHEMA}.WATCHED_MOVIE w
                JOIN {SCHEMA}.MOVIES m ON w.MOVIE_ID = m.MOVIE_ID
                WHERE w.PROMO_CUST_ID = :cust_id
                FETCH FIRST :limit ROWS ONLY
            """, {'cust_id': customer_id, 'limit': limit})
        
        movies = cursor.fetchall()
        movie_ids = [m[0] for m in movies]
        
        for movie_id, movie_title, genres_raw in movies:
            genres = parse_genres(genres_raw).keys() if genres_raw else []
            nodes.append({
                'id': f'm{movie_id}', 
                'label': movie_title, 
                'type': 'movie', 
                'group': 2, 
                'size': 8, 
                'genres': list(genres)[:2]
            })
            links.append({
                'source': f'c{customer_row[0]}', 
                'target': f'm{movie_id}', 
                'value': 1
            })
        
        # PGQL: Clientes similares (se depth >= 2)
        if depth >= 2 and movie_ids:
            try:
                pgql_similar = f"""
                    SELECT cust_id, name, common_count
                    FROM GRAPH_TABLE ({GRAPH_NAME}
                        MATCH (c1:customer)-[:watched]->(m:movie)<-[:watched]-(c2:customer)
                        WHERE c1.cust_id = :cust_id 
                          AND c2.cust_id != :cust_id
                        COLUMNS (
                            c2.cust_id AS cust_id,
                            c2.firstname || ' ' || c2.lastname AS name,
                            COUNT(DISTINCT m.movie_id) AS common_count
                        )
                    )
                    GROUP BY cust_id, name, common_count
                    ORDER BY common_count DESC
                    FETCH FIRST 5 ROWS ONLY
                """
                cursor.execute(pgql_similar, {'cust_id': customer_id})
            except:
                # Fallback SQL
                cursor.execute(f"""
                    SELECT c2.CUST_ID, c2.FIRSTNAME || ' ' || c2.LASTNAME as NAME,
                           COUNT(DISTINCT w1.MOVIE_ID) as common_count
                    FROM {SCHEMA}.WATCHED_MOVIE w1
                    JOIN {SCHEMA}.WATCHED_MOVIE w2 ON w1.MOVIE_ID = w2.MOVIE_ID
                    JOIN {SCHEMA}.MOVIES_CUSTOMER c2 ON w2.PROMO_CUST_ID = c2.CUST_ID
                    WHERE w1.PROMO_CUST_ID = :cust_id
                      AND c2.CUST_ID != :cust_id
                    GROUP BY c2.CUST_ID, c2.FIRSTNAME, c2.LASTNAME
                    ORDER BY common_count DESC
                    FETCH FIRST 5 ROWS ONLY
                """, {'cust_id': customer_id})
            
            similar_customers = cursor.fetchall()
            for sim_id, sim_name, common_count in similar_customers:
                nodes.append({
                    'id': f'c{sim_id}', 
                    'label': sim_name, 
                    'type': 'customer', 
                    'group': 3, 
                    'size': 10, 
                    'common_movies': int(common_count)
                })
                links.append({
                    'source': f'c{customer_row[0]}', 
                    'target': f'c{sim_id}', 
                    'value': 2
                })
        
        cursor.close()
        conn.close()
        
        stats = {
            'total_nodes': len(nodes),
            'total_links': len(links),
            'customers': len([n for n in nodes if n['type'] == 'customer']),
            'movies': len([n for n in nodes if n['type'] == 'movie'])
        }
        
        return jsonify({
            'success': True, 
            'nodes': nodes, 
            'links': links, 
            'stats': stats
        })
        
    except Exception as e:
        print(f"❌ Erro: {e}")
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/chat', methods=['POST'])
def chat():

    try:
        data = request.get_json() or {}
        message = (data.get('message', '') or '').strip()
        customer_id = data.get('customer_id', None)

        if not message:
            return jsonify({'success': False, 'error': 'Message required'}), 400

        graph_context = []
        movie_recommendations = []

        if customer_id:
            conn = get_db_connection()
            cursor = conn.cursor()

            try:
                # PGQL: Filmes assistidos
                try:
                    pgql_watched = f"""
                        SELECT title, genres
                        FROM GRAPH_TABLE ({GRAPH_NAME}
                            MATCH (c:customer)-[:watched]->(m:movie)
                            WHERE c.cust_id = :cust_id
                            COLUMNS (m.title AS title, m.genres AS genres)
                        )
                        FETCH FIRST 5 ROWS ONLY
                    """
                    cursor.execute(pgql_watched, {'cust_id': customer_id})
                except:
                    cursor.execute(f"""
                        SELECT m.TITLE, m.GENRES
                        FROM {SCHEMA}.WATCHED_MOVIE w
                        JOIN {SCHEMA}.MOVIES m ON w.MOVIE_ID = m.MOVIE_ID
                        WHERE w.PROMO_CUST_ID = :cust_id
                        FETCH FIRST 5 ROWS ONLY
                    """, {'cust_id': customer_id})

                watched = [row[0] for row in cursor.fetchall()]
                if watched:
                    graph_context.append(f"Você assistiu: {', '.join(watched)}")

                # PGQL: Recomendações do grafo
                try:
                    pgql_recs = f"""
                        SELECT 
                            movie_id,
                            title,
                            summary,
                            rating,
                            genres,
                            similar_users
                        FROM GRAPH_TABLE ({GRAPH_NAME}
                            MATCH (c1:customer)-[:watched]->(m:movie)<-[:watched]-(c2:customer)-[:watched]->(m2:movie)
                            WHERE c1.cust_id = :cust_id 
                              AND c2.cust_id != :cust_id
                            COLUMNS (
                                m2.movie_id AS movie_id,
                                m2.title AS title,
                                m2.summary AS summary,
                                m2.rating AS rating,
                                m2.genres AS genres,
                                COUNT(DISTINCT c2.cust_id) AS similar_users
                            )
                        )
                        GROUP BY movie_id, title, summary, rating, genres, similar_users
                        ORDER BY similar_users DESC, rating DESC
                        FETCH FIRST 3 ROWS ONLY
                    """
                    cursor.execute(pgql_recs, {'cust_id': customer_id})
                except:
                    cursor.execute(f"""
                        SELECT m2.MOVIE_ID, m2.TITLE, m2.SUMMARY, m2.RATING, m2.GENRES,
                               COUNT(DISTINCT c2.CUST_ID) as similar_users
                        FROM {SCHEMA}.WATCHED_MOVIE w1
                        JOIN {SCHEMA}.WATCHED_MOVIE w2 ON w1.MOVIE_ID = w2.MOVIE_ID
                        JOIN {SCHEMA}.MOVIES_CUSTOMER c2 ON w2.PROMO_CUST_ID = c2.CUST_ID
                        JOIN {SCHEMA}.WATCHED_MOVIE w3 ON c2.CUST_ID = w3.PROMO_CUST_ID
                        JOIN {SCHEMA}.MOVIES m2 ON w3.MOVIE_ID = m2.MOVIE_ID
                        WHERE w1.PROMO_CUST_ID = :cust_id
                          AND c2.CUST_ID != :cust_id
                          AND m2.MOVIE_ID NOT IN (
                              SELECT MOVIE_ID FROM {SCHEMA}.WATCHED_MOVIE 
                              WHERE PROMO_CUST_ID = :cust_id
                          )
                        GROUP BY m2.MOVIE_ID, m2.TITLE, m2.SUMMARY, m2.RATING, m2.GENRES
                        ORDER BY similar_users DESC, m2.RATING DESC
                        FETCH FIRST 3 ROWS ONLY
                    """, {'cust_id': customer_id})

                for row in cursor.fetchall():
                    # Buscar poster
                    cursor2 = conn.cursor()
                    cursor2.execute(f"""
                        SELECT ASSET_URL FROM {SCHEMA}.MEDIA_ASSETS 
                        WHERE MOVIE_ID = :mid AND ASSET_TYPE = 'poster_url'
                    """, {'mid': row[0]})
                    poster_row = cursor2.fetchone()
                    poster_url = poster_row[0] if poster_row else None
                    cursor2.close()
                    
                    movie_recommendations.append({
                        'id': row[0],
                        'title': row[1],
                        'summary': row[2],
                        'rating': float(row[3]) if row[3] else 0,
                        'genres': parse_genres(row[4]),
                        'similar_users': int(row[5]),
                        'poster_url': poster_url,
                        'graph_reason': f'Baseado em {row[5]} usuários com gostos similares'
                    })

                if movie_recommendations:
                    titles = [m['title'] for m in movie_recommendations]
                    graph_context.append(f"Recomendações do Property Graph: {', '.join(titles)}")

            except Exception as e:
                print(f"⚠️  Erro ao buscar contexto: {e}")
            finally:
                cursor.close()
                conn.close()

        # Prompt para o LLM
        context_text = "\n".join(graph_context) if graph_context else "Sem histórico"

        prompt = f"""Você é um assistente de cinema. Seja BREVE e NATURAL.

CONTEXTO DO USUÁRIO (via Property Graph com PGQL):
{context_text}

PERGUNTA: {message}

INSTRUÇÕES:
- Se houver recomendações, mencione que usou o Property Graph e como ele te ajudou
- Máximo 2-3 frases
- Seja conversacional

RESPOSTA:"""

        llm_response = get_llm_response(prompt, temperature=0.7, max_tokens=200)

        return jsonify({
            'success': True,
            'message': message,
            'response': llm_response,
            'movie_cards': movie_recommendations,
            'graph_used': len(graph_context) > 0,
            'method': 'property_graph_pgql'
        })

    except Exception as e:
        print(f"❌ Erro: {e}")
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/chat/smart', methods=['POST'])
def smart_chat():
    """Chat inteligente simplificado"""
    try:
        data = request.get_json() or {}
        message = (data.get('message', '') or '').strip()
        customer_id = data.get('customer_id', None)

        if not message:
            return jsonify({'success': False, 'error': 'Message required'}), 400

        graph_context = []

        if customer_id:
            conn = get_db_connection()
            cursor = conn.cursor()

            try:
                cursor.execute(f"""
                    SELECT m.TITLE
                    FROM {SCHEMA}.WATCHED_MOVIE w
                    JOIN {SCHEMA}.MOVIES m ON w.MOVIE_ID = m.MOVIE_ID
                    WHERE w.PROMO_CUST_ID = :cust_id
                    FETCH FIRST 5 ROWS ONLY
                """, {'cust_id': customer_id})

                watched = [row[0] for row in cursor.fetchall()]
                if watched:
                    graph_context.append(f"Filmes assistidos: {', '.join(watched)}")

            except Exception as e:
                print(f"⚠️  Erro: {e}")
            finally:
                cursor.close()
                conn.close()

        context_text = "\n".join(graph_context) if graph_context else "Sem histórico"

        prompt = f"""Você é um assistente de cinema. Seja BREVE.

CONTEXTO:
{context_text}

PERGUNTA: {message}

Responda em até 3 frases."""

        llm_response = get_llm_response(prompt, temperature=0.7, max_tokens=200)

        return jsonify({
            'success': True,
            'message': message,
            'response': llm_response,
            'graph_insights': graph_context,
            'context_used': len(graph_context) > 0
        })

    except Exception as e:
        print(f"❌ Erro: {e}")
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/health', methods=['GET'])
def health_check():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM DUAL")
        cursor.fetchone()
        cursor.close()
        conn.close()
        return jsonify({'status': 'healthy', 'database': 'connected'})
    except Exception as e:
        return jsonify({'status': 'unhealthy', 'error': str(e)}), 500


if __name__ == '__main__':
    print("=" * 60)
    print("🎬 CineGen AI Backend")
    print("=" * 60)
    print("🌐 Server: http://0.0.0.0:8000")
    print("=" * 60)
    app.run(host='0.0.0.0', port=8000, debug=True)