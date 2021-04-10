import json
import boto3
from botocore.exceptions import ClientError
##SQS settings
sqs = boto3.client('sqs')
#queue_url = 'https://sqs.eu-central-1.amazonaws.com/131202687xxxx/my-reality-XXXXXX' uncomment and add queue url

def get_messages_from_queue(queue_url):
    """Generates messages from an SQS queue.

    Note: this continues to generate messages until the queue is empty.
    Every message on the queue will be deleted.

    :param queue_url: URL of the SQS queue to drain.

    """
    while True:
        resp = sqs.receive_message(
            QueueUrl=queue_url,
            AttributeNames=['All'],
            MessageAttributeNames=['Transport','Type'],
            MaxNumberOfMessages=10
        )

        try:
            yield from resp['Messages']
        except KeyError:
            return

        entries = [
            {'Id': msg['MessageId'], 'ReceiptHandle': msg['ReceiptHandle']}
            for msg in resp['Messages']
        ]

        resp = sqs.delete_message_batch(
            QueueUrl=queue_url, Entries=entries
        )

        if len(resp['Successful']) != len(entries):
            raise RuntimeError(
                f"Failed to delete messages: entries={entries!r} resp={resp!r}"
            )

def handler(event, context):

    #####SES settings
    # This address must be verified with Amazon SES.
    SENDER = "Sender Name <XXXXX@gmail.com>"
    # Replace recipient@example.com with a "To" address. If your account 
# is still in the sandbox, this address must be verified.
    RECIPIENT = "XXXXX@gmail.com"
# Specify a configuration set. If you do not want to use a configuration
# set, comment the following variable, and the 
# ConfigurationSetName=CONFIGURATION_SET argument below.
    #CONFIGURATION_SET = "ConfigSet"
# If necessary, replace us-west-2 with the AWS Region you're using for Amazon SES.
    AWS_REGION = "eu-central-1"
# The subject line for the email.
    SUBJECT = "Reality Caller"
# The email body for recipients with non-HTML email clients.
    BODY_TEXT = ("Nove reality \r\n"
             "Neni html, nic neuvidis"
            )
       

# The character encoding for the email.
    CHARSET = "UTF-8"

# Create a new SES resource and specify a region.
    client = boto3.client('ses',region_name=AWS_REGION)
    htmlbody = []
      
    print('Loading function')
    for message in get_messages_from_queue(queue_url):
        htmlbody.append('<p>'+ message['Body'].replace("\n","<br>") + " " + message['MessageAttributes']['Transport']['StringValue'].replace("\n","<br>") + '</p>')
        #if message.MessageAttributes is not None:
        #    author_name = message.MessageAttributes.get('Transport').get('StringValue')
        print(json.dumps(message['MessageAttributes']['Transport']['StringValue']))
      #  else: print('je to v pici')
    

       
   # return {
    #    'statusCode': 200 ,'body': json.dumps(message)
   # }
    
    ###Mail
    listToStr = '\n\r'.join(map(str, htmlbody)) 
    BODY_HTML = """<html>
                <head></head>
                <body>
                <h1>Amazon SES Test (SDK for Python)</h1>
                """ + listToStr + """
                </body>
                </html>
                """ 
    try:
    #Provide the contents of the email.
        mailresponse = client.send_email(
        Destination={
            'ToAddresses': [
                RECIPIENT,
            ],
        },
        Message={
            'Body': {
                'Html': {
                    'Charset': CHARSET,
                    'Data': BODY_HTML,
                },
                'Text': {
                    'Charset': CHARSET,
                    'Data': BODY_TEXT,
                },
            },
            'Subject': {
                'Charset': CHARSET,
                'Data': SUBJECT,
            },
        },
        Source=SENDER,
        # If you are not using a configuration set, comment or delete the
        # following line
       # ConfigurationSetName=CONFIGURATION_SET,
    )
# Display an error if something goes wrong.	
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(mailresponse['MessageId'])

       
    return {
        'statusCode': 200 #'body': json.dumps(message['Body'])
    }
