import json
import pymysql
import os

def lambda_handler(event, context):
    # Database connection parameters
    host = os.environ['DB_HOST']
    user = os.environ['DB_USER']
    password = os.environ['DB_PASSWORD']
    database = os.environ['DB_DATABASE']

    # Create a connection to the databasechange
    connection = pymysql.connect(host=host, user=user, password=password, db=database)

    try:
        operation = event.get('operation')

        if operation == 'CREATE':
            response = create_record(event, connection)
        elif operation == 'READ':
            response = read_record(event, connection)
        elif operation == 'UPDATE':
            response = update_record(event, connection)
        elif operation == 'DELETE':
            response = delete_record(event, connection)
        else:
            response = {
                'statusCode': 400,
                'body': json.dumps('Error: operation not supported')
            }

    except Exception as e:
        print('Error: ', e)
        response = {
            'statusCode': 400,
            'body': json.dumps('Error: internal server error!')
        }
    finally:
        connection.close()

    return response

def create_record(data, connection):
    with connection.cursor() as cursor:
        sql = 'INSERT INTO your_table (column1, column2) VALUES (%s, %s)'
        cursor.execute(sql, (data['column1'], data['column2']))
        connection.commit()
        return {
            'statusCode': 200,
            'body': json.dumps({'insertId': cursor.lastrowid})
        }

def read_record(data, connection):
    with connection.cursor() as cursor:
        sql = 'SELECT * FROM your_table WHERE id = %s'
        cursor.execute(sql, (data['id'],))
        result = cursor.fetchone()
        return {
            'statusCode': 200,
            'body': json.dumps(result)
        }

def update_record(data, connection):
    with connection.cursor() as cursor:
        sql = 'UPDATE your_table SET column1 = %s, column2 = %s WHERE id = %s'
        cursor.execute(sql, (data['column1'], data['column2'], data['id']))
        connection.commit()
        return {
            'statusCode': 200,
            'body': json.dumps({'affectedRows': cursor.rowcount})
        }

def delete_record(data, connection):
    with connection.cursor() as cursor:
        sql = 'DELETE FROM your_table WHERE id = %s'
        cursor.execute(sql, (data['id'],))
        connection.commit()
        return {
            'statusCode': 200,
            'body': json.dumps({'affectedRows': cursor.rowcount})
        }
