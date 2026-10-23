############### 16/07/2026 #################
import json
import boto3
import hashlib
import uuid
import os
from datetime import datetime, timedelta

# PyJWT must be bundled in the deployment package. If it isn't, importing it
# would crash the whole module at cold start and API Gateway would return an
# opaque 502 with no log. Import defensively instead so the module always loads
# and the handler can return a clear, logged error.
try:
    import jwt
except ImportError:
    jwt = None

dynamodb = boto3.resource('dynamodb')
# Read table name with a safe default. A missing USERS_TABLE env var used to
# raise KeyError at import time (-> 502 init crash). With a default the module
# always loads; a wrong/missing table surfaces as a clean handled 500 instead.
USERS_TABLE = os.environ.get('USERS_TABLE', 'v2_Users')
users_table = dynamodb.Table(USERS_TABLE)
s3_client = boto3.client('s3')

AUDIT_S3_BUCKET = os.environ.get('AUDIT_S3_BUCKET', 'test-v2-andretti-audit-logs')
AUDIT_S3_PREFIX = os.environ.get('AUDIT_S3_PREFIX', 'audit-logs/')
SESSION_LOGS_PREFIX = os.environ.get('SESSION_LOGS_PREFIX', 'session_logs/')
SYSTEM_LOGS_PREFIX = os.environ.get('SYSTEM_LOGS_PREFIX', 'system_logs/')
SESSION_SCOPED_ENTITIES = {'pipeline', 'session', 'session_config', 'recording'}


def record_audit_event(action, entity, entity_id, created_by, details=None):
    """Write an audit-trail record to S3 in three places: a date partition
    (unchanged), session_logs/{session_id}/ for session-scoped entities, or
    system_logs/YYYY/MM/DD/ otherwise (auth events land here).

    Same schema/layout as config_management.py's record_audit_event, so both
    Lambdas' events show up together when read back. No-op if AUDIT_S3_BUCKET
    is not configured; never raises - audit logging must not break auth."""
    if not AUDIT_S3_BUCKET:
        return
    try:
        now = datetime.utcnow()
        record = {
            'audit_id': str(uuid.uuid4()),
            'timestamp': now.isoformat() + 'Z',
            'action': action,
            'entity': entity,
            'entity_id': str(entity_id) if entity_id is not None else '',
            'created_by': created_by or 'unknown',
            'details': details or {}
        }
        suffix = f"{action}_{int(now.timestamp())}_{record['audit_id'][:8]}.json"
        body = json.dumps(record, default=str).encode('utf-8')
        date_path = now.strftime('%Y/%m/%d')
        s3_client.put_object(
            Bucket=AUDIT_S3_BUCKET,
            Key=f"{AUDIT_S3_PREFIX}{date_path}/{suffix}",
            Body=body,
            ContentType='application/json'
        )
        if entity in SESSION_SCOPED_ENTITIES and record['entity_id']:
            s3_client.put_object(
                Bucket=AUDIT_S3_BUCKET,
                Key=f"{SESSION_LOGS_PREFIX}{record['entity_id']}/{suffix}",
                Body=body,
                ContentType='application/json'
            )
        else:
            s3_client.put_object(
                Bucket=AUDIT_S3_BUCKET,
                Key=f"{SYSTEM_LOGS_PREFIX}{date_path}/{suffix}",
                Body=body,
                ContentType='application/json'
            )
    except Exception as e:
        print(f"[AUDIT] Failed to record {action} for {entity}:{entity_id}: {e}")


def hash_password(password):
    """Hash password using SHA-256"""
    return hashlib.sha256(password.encode()).hexdigest()

def generate_token(user_id, email, username):
    """Generate JWT token.

    Raises a clear RuntimeError (caught by the callers' try/except -> handled
    500 with a log line) instead of crashing with an opaque error if PyJWT was
    not bundled or JWT_SECRET is not configured."""
    if jwt is None:
        raise RuntimeError("PyJWT is not installed in the deployment package")
    secret_key = os.environ.get('JWT_SECRET')
    if not secret_key:
        raise RuntimeError("JWT_SECRET environment variable is not set")
    payload = {
        'user_id': user_id,
        'email': email,
        'username': username,
        'exp': datetime.utcnow() + timedelta(hours=24)
    }
    return jwt.encode(payload, secret_key, algorithm='HS256')

def validate_email(email):
    """Basic email validation"""
    import re
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, email) is not None

def validate_username(username):
    """Username validation - alphanumeric and underscore only"""
    import re
    pattern = r'^[a-zA-Z0-9_]{3,20}$'
    return re.match(pattern, username) is not None

def signup(body):
    """Handle user signup"""
    try:
        email = body.get('email', '').lower().strip()
        username = body.get('username', '').lower().strip()
        password = body.get('password', '')
        
        if not email or not username or not password:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Email, username, and password are required'})
            }
        
        if not validate_email(email):
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Invalid email format'})
            }
        
        if not validate_username(username):
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Username must be 3-20 characters (letters, numbers, underscore only)'})
            }
        
        if len(password) < 6:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Password must be at least 6 characters'})
            }
        
        email_response = users_table.get_item(Key={'email': email})
        if 'Item' in email_response:
            return {
                'statusCode': 409,
                'body': json.dumps({'message': 'Email already exists'})
            }
        
        username_response = users_table.query(
            IndexName='username-index',
            KeyConditionExpression='username = :username',
            ExpressionAttributeValues={
                ':username': username
            }
        )
        
        if username_response['Items']:
            return {
                'statusCode': 409,
                'body': json.dumps({'message': 'Username already exists'})
            }
        
        user_id = str(uuid.uuid4())
        hashed_password = hash_password(password)
        
        users_table.put_item(
            Item={
                'email': email,
                'username': username,
                'user_id': user_id,
                'password': hashed_password,
                'created_at': datetime.utcnow().isoformat(),
                'updated_at': datetime.utcnow().isoformat()
            }
        )
        
        token = generate_token(user_id, email, username)

        record_audit_event('SIGNUP', 'user', user_id, username, {'email': email})

        return {
            'statusCode': 201,
            'body': json.dumps({
                'message': 'User created successfully',
                'token': token,
                'user': {
                    'user_id': user_id,
                    'email': email,
                    'username': username
                }
            })
        }
        
    except Exception as e:
        print(f"Signup Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Internal server error'})
        }

def login(body):
    """Handle user login with email or username"""
    try:
        identifier = body.get('email') or body.get('username', '')
        identifier = identifier.lower().strip()
        password = body.get('password', '')
        
        if not identifier or not password:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Email/Username and password are required'})
            }
        
        user = None
        
        if '@' in identifier:
            response = users_table.get_item(Key={'email': identifier})
            if 'Item' in response:
                user = response['Item']
        else:
            response = users_table.query(
                IndexName='username-index',
                KeyConditionExpression='username = :username',
                ExpressionAttributeValues={
                    ':username': identifier
                }
            )
            if response['Items']:
                user = response['Items'][0]
        
        if not user:
            record_audit_event('LOGIN_FAILED', 'user', identifier, identifier,
                                {'reason': 'no matching account'})
            return {
                'statusCode': 401,
                'body': json.dumps({'message': 'Invalid credentials'})
            }

        hashed_password = hash_password(password)
        if user['password'] != hashed_password:
            record_audit_event('LOGIN_FAILED', 'user', user.get('user_id'), identifier,
                                {'reason': 'bad password'})
            return {
                'statusCode': 401,
                'body': json.dumps({'message': 'Invalid credentials'})
            }

        token = generate_token(user['user_id'], user['email'], user['username'])
        
        users_table.update_item(
            Key={'email': user['email']},
            UpdateExpression='SET last_login = :login_time',
            ExpressionAttributeValues={
                ':login_time': datetime.utcnow().isoformat()
            }
        )

        record_audit_event('LOGIN_SUCCESS', 'user', user['user_id'], user['username'])

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Login successful',
                'token': token,
                'user': {
                    'user_id': user['user_id'],
                    'email': user['email'],
                    'username': user['username']
                }
            })
        }
        
    except Exception as e:
        print(f"Login Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Internal server error'})
        }
def lambda_handler(event, context):
    """Main Lambda handler"""
    try:
        print(f"Received event: {json.dumps(event)}")
        
        http_method = event.get('httpMethod', '')
        path = event.get('path', '')
        
        body_data = event.get('body', '{}')
        
        if isinstance(body_data, dict):
            body = body_data
        elif isinstance(body_data, str):
            body = json.loads(body_data) if body_data else {}
        else:
            body = {}
        
        print(f"Parsed body: {body}")
        
        if path == '/auth/signup' and http_method == 'POST':
            response = signup(body)
        elif path == '/auth/login' and http_method == 'POST':
            response = login(body)
        else:
            response = {
                'statusCode': 404,
                'body': json.dumps({'message': 'Route not found'})
            }
        
        response['headers'] = {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type,Authorization',
            'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS'
        }
        
        return response
        
    except json.JSONDecodeError as e:
        print(f"JSON Decode Error: {str(e)}")
        return {
            'statusCode': 400,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({'message': 'Invalid JSON in request body'})
        }
    except Exception as e:
        print(f"Lambda Handler Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({'message': 'Internal server error'})
        }


