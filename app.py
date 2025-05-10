from datetime import datetime, UTC
from flask import Flask, jsonify, request, make_response
from faker import Faker
import logging
import os
import uuid
import psycopg2

# OpenTelemetry Instrumentation
from opentelemetry.instrumentation.psycopg2 import Psycopg2Instrumentor
Psycopg2Instrumentor().instrument(enable_commenter=True, commenter_options={})
# End of OpenTelemetry Instrumentation

# System Performance
from opentelemetry.metrics import set_meter_provider
from opentelemetry.instrumentation.system_metrics import SystemMetricsInstrumentor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import ConsoleMetricExporter, PeriodicExportingMetricReader
exporter = ConsoleMetricExporter()
set_meter_provider(MeterProvider([PeriodicExportingMetricReader(exporter)]))
SystemMetricsInstrumentor().instrument()
configuration = {
    "system.memory.usage": ["used", "free", "cached"],
    "system.cpu.time": ["idle", "user", "system", "irq"],
    "system.network.io": ["transmit", "receive"],
    "process.memory.usage": None,
    "process.memory.virtual": None,
    "process.cpu.time": ["user", "system"],
    "process.context_switches": ["involuntary", "voluntary"],
}
# end of System Performance

log_level = os.getenv("APP_LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, log_level, logging.INFO))
logging.getLogger('werkzeug').setLevel(getattr(logging, log_level, logging.INFO))
app = Flask(__name__)
redis_client = None
INTERNAL_ERROR = "INTERNAL SERVER ERROR"


def get_env_variable(var_name, default=None):
    value = os.environ.get(var_name)
    if value is None:
        if default is not None:
            return default
        else:
            raise ValueError(f"Environment variable '{var_name}' not set.")
    return value

def request_log(component: str, payload:dict = None ):
    transaction_id = str(uuid.uuid4())
    request_message = {
        'message': 'Request',
        'component': component,
        'transactionId': transaction_id
    }
    if payload:
        request_message['payload'] = payload
    logging.info(request_message)
    return transaction_id


def response_log(transaction_id:str, component: str, return_code, payload:dict = None):
    response_message = {
        'message': 'Response',
        'component': component,
        'transactionId': transaction_id,
        'returnCode': return_code
    }
    if payload:
        response_message['payload'] = payload
    logging.info(response_message)

def connect_to_database():
    db_host = get_env_variable("POSTGRES_HOST")
    db_port = get_env_variable("POSTGRES_PORT")
    db_name = get_env_variable("POSTGRES_DB")
    db_user = get_env_variable("POSTGRES_USER")
    db_password = get_env_variable("POSTGRES_PASSWORD")

    try:
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password,
        )
        logging.info("Successfully connected to PostgreSQL")
        return conn
    except psycopg2.Error as e:
        logging.error(f"Error connecting to PostgreSQL: {e}")
        return None


def member_search(user_id: str):
    user = None
    conn = None
    cursor = None
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT user_id, first_name, last_name 
            FROM members 
            WHERE user_id = %s
        """, (user_id,))

        result = cursor.fetchone()
        if result:
            user = {
                'userId': user_id,
                'firstName': result[1],
                'lastName': result[2]
            }
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    return user

def member_create(user_id: str):
    conn = None
    cursor = None
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        # For demo purposes we will just make up a member
        fake = Faker()
        first_name = fake.first_name()
        last_name = fake.last_name()
        user = {
            'userId': user_id,
            'firstName': first_name,
            'lastName': last_name
        }

        cursor.execute("INSERT INTO members (user_id, first_name, last_name) VALUES (%s, %s, %s);",
                       (user['userId'], user['firstName'], user['lastName']))
        conn.commit()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    return user


def member_service(user_id):
    user = member_search(user_id)
    if not user:
        user = member_create(user_id)
    return user

@app.route("/member", methods=["POST"])
def member():
    return_code = 200
    component = 'member'
    transaction_id = None
    try:
        data = request.get_json()
        user_id = data.get("userId", None)
        payload = {
            'userId': user_id,
        }
        transaction_id = request_log(component, payload)
        if not user_id:
            return_code = 400
        else:
            user = member_service(user_id)
            if not user:
                return_code = 401
            else:
                payload = user
    except Exception as ex:
        return_code = 500
        payload = {"error": INTERNAL_ERROR, "details": str(ex)}
    response_log(transaction_id, component, return_code, payload)
    return make_response(jsonify(payload), return_code)

