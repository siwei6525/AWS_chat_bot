import boto3
import json

client = boto3.client('lex-runtime')


def lambda_handler(event, context):
    print (event);
    
    last_user_message = event['messages'][0]['unstructured']['text'];
    print (last_user_message);
    
    botMessage = "Please try again.";
    
    
    
    user = "none"
    

    if last_user_message is None or len(last_user_message) < 1:
        return {
            'statusCode': 200,
            'body': json.dumps(botMessage)
        }

    response = client.post_text(botName='chatBot',
                                botAlias='botOne',
                                userId=user,
                                inputText=last_user_message)

    if response['message'] is not None or len(response['message']) > 0:
        last_user_message = response['message']
    
    print ("response: ");
    print (last_user_message);

    
    return {
        'statusCode': 200,
        'messages': last_user_message
    }
