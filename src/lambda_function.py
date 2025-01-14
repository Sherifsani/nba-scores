import json
import urllib.request
import boto3
import datetime
import os
from botocore.config import Config

def lambda_handler(event, context):
    # Configure boto3 with shorter timeout
    config = Config(
        connect_timeout=2,
        read_timeout=2
    )
    sns = boto3.client('sns', config=config)
    
    # Get environment variables
    API_KEY = os.getenv('API_KEY')
    SNS_TOPIC_ARN = os.getenv('SNS_TOPIC_ARN')
    
    today_date = datetime.datetime.now().strftime("%Y-%m-%d")
    print(f"fetching games for {today_date}")
    
    api_url = f"https://v1.basketball.api-sports.io/games?league=12&season=2024-2025&date={today_date}"
    headers = {
        'x-rapidapi-host': "v1.basketball.api-sports.io",
        'x-rapidapi-key': API_KEY
    }
    
    # Set timeout for the HTTP request
    req = urllib.request.Request(api_url, headers=headers, method="GET")
    
    try:
        # Send the HTTP request with timeout
        with urllib.request.urlopen(req, timeout=2) as response:
            response_data = response.read().decode('utf-8')
            data = json.loads(response_data)
            
            # Process games more efficiently using list comprehension
            messages = [
                f"{game['teams']['home']['name']} ({game['scores']['home']['total']}) vs "
                f"{game['teams']['away']['name']} ({game['scores']['away']['total']})"
                for game in data.get('response', [])
            ]
            
            if messages:
                final_message = "\n".join(messages)
                sns.publish(
                    TopicArn=SNS_TOPIC_ARN,
                    Message=final_message,
                    Subject=f"Games for {today_date}"
                )
                return {
                    'statusCode': 200,
                    'body': json.dumps({'message': 'Successfully processed games'})
                }
            else:
                return {
                    'statusCode': 200,
                    'body': json.dumps({'message': 'No games found for today'})
                }
                
    except urllib.error.URLError as e:
        error_message = f"Error fetching games: {e.reason}"
        print(error_message)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': error_message})
        }
    except urllib.error.HTTPError as e:
        error_message = f"HTTP error: {e.code} - {e.reason}"
        print(error_message)
        return {
            'statusCode': e.code,
            'body': json.dumps({'error': error_message})
        }
    except Exception as e:
        error_message = f"Unexpected error: {str(e)}"
        print(error_message)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': error_message})
        }