import json
import boto3
import decimal
import logging
import os

#queue_url = 'https://sqs.eu-central-1.amazonaws.com/131202687XXX/my-realityXXXXXXX' # add url from SQS

sqs = boto3.client('sqs') #client needed , not just resource

print('Loading function')

def handler(event, context):
	print('------------------------')
#print(event)
	#1. Iterate over each record
	try:
		for record in event['Records']:
			#2. Handle event by type
			if record['eventName'] == 'INSERT':
				handle_insert(record)
			elif record['eventName'] == 'MODIFY':
				handle_modify(record)
			elif record['eventName'] == 'REMOVE':
				handle_remove(record)
		print('------------------------')
		

		message = ( "Totally "
             + str(1245)
             + " items found.Script finished successfully ...\n Ended at "
             #+ end_time.strftime("%Y-%m-%d %H:%M:%S")
                  )
		return {"message": message}
		
	except Exception as e: 
		print(e)
		print('------------------------')
		return {"message":  "Error"+str(e)}


def handle_insert(record):
	print("Handling INSERT Event")
	
	#3a. Get newImage content
	newImage = record['dynamodb']['NewImage']
	
	#3b. Parse values
	newReality = newImage['parsed']['M']['hyperlink']['S']
	popis      = newImage['meta_description']['S']
	transport =  newImage['Transport']['M']
	transport_txt = makestring(transport)


	#3c. Print it
	resp = "New Reality=> " + "\n " + str(popis).replace("\\xa0", " ").replace(";","\n")+ "\n " + newReality 
	response = sqs.send_message(QueueUrl=queue_url, MessageBody=resp,MessageAttributes={
		    'Type': {
            'DataType': 'String',
            'StringValue': 'NewReality'},
			'Transport': {
            'StringValue': transport_txt,
            'DataType': 'String'}
        })
	print(resp)



def handle_modify(record):
	print("Handling MODIFY Event")


	#3a. Parse oldImage and price
	oldImage = record['dynamodb']['OldImage']
	oldPrice = oldImage['price_czk']['M']['value_raw']['N']
	
	#3b. Parse newImage and price
	newImage = record['dynamodb']['NewImage']
	newPrice = newImage['price_czk']['M']['value_raw']['N']
	
	UpdatedReality = newImage['parsed']['M']['hyperlink']['S']

	#3c. Check for change
	if oldPrice != newPrice:
		resp='Price changed - old one= ' + str(oldPrice) + ', newPrice= ' + str(newPrice) +"\n " + UpdatedReality
		response = sqs.send_message(QueueUrl=queue_url, MessageBody=resp,MessageAttributes={
        'Type': {
            'DataType': 'String',
            'StringValue': 'UpdatedReality'
        }})
		print(resp)

	print("Done handling MODIFY Event")

def handle_remove(record):
	print("Handling REMOVE Event")

	#3a. Parse oldImage
	oldImage = record['dynamodb']['OldImage']
	
	#3b. Parse values
	oldRealityId = oldImage['hash_id']['N']

	#3c. Print it
	print ('Row removed with hash_id=' + oldRealityId)

	print("Done handling REMOVE Event")
def makestring (par):
	record=par
	if 'S' in record['hradcanska']['M']['driving'] :
				hradcanska_D='Hradcanska - Drive: ZERO_RESULTS  \n'
	else:
				hradcanska_D = ('Hradcanska - Drive: ' + record['hradcanska']['M']['driving']['M']['distance']['M']['text']['S'] +' '
			                            			   + record['hradcanska']['M']['driving']['M']['duration']['M']['text']['S'] + '\n' )
	if 'S' in record['hradcanska']['M']['transit']  :
				hradcanska_P='Hradcanska - Public: ZERO_RESULTS'
	else:		                            
				hradcanska_P = ('Hradcanska - Public:' + record['hradcanska']['M']['transit']['M']['distance']['M']['text']['S'] +' '
		                            				   + record['hradcanska']['M']['transit']['M']['duration']['M']['text']['S']  )
		                            
	hradcanska=hradcanska_D + hradcanska_P
	
	if 'S' in record['msd']['M']['driving']:
				msd_D='MSD - Drive: ZERO_RESULTS  \n'
	else:
				msd_D = ('MSD - Drive: ' + record['msd']['M']['driving']['M']['distance']['M']['text']['S'] +' '
			                             + record['msd']['M']['driving']['M']['duration']['M']['text']['S'] + '\n' )
	if 'S' in record['msd']['M']['transit'] :
				msd_P=' MSD- Public: ZERO_RESULTS'
	else:		                            
				msd_P = ('MSD- Public:' + record['msd']['M']['transit']['M']['distance']['M']['text']['S'] +' '
		                            	+ record['msd']['M']['transit']['M']['duration']['M']['text']['S']  )
		                            
	msd= msd_D + msd_P
	
	if 'S' in record['liberec']['M']['driving']:
				liberec_P='Liberec - Drive: ZERO_RESULTS \n'
	else:
				liberec_D = ('Liberec - Drive ' + record['liberec']['M']['driving']['M']['distance']['M']['text']['S'] +' '
			                            	+ record['liberec']['M']['driving']['M']['duration']['M']['text']['S'] + '\n' )
	if  'S' in  record['liberec']['M']['transit'] :
				liberec_P='Liberec - Public: ZERO_RESULTS'
	else:		                            
				liberec_P = ('Liberec - Public:' + record['liberec']['M']['transit']['M']['distance']['M']['text']['S'] +' '
		                            		 + record['liberec']['M']['transit']['M']['duration']['M']['text']['S']  )
		                            
	liberec=liberec_D + liberec_P
	
	response= '   \n ' + hradcanska + '\n' + msd + '\n' + liberec
	
	return(response)
	


