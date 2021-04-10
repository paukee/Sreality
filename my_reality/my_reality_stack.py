from aws_cdk import (
    core,
    aws_lambda,
    aws_lambda_destinations,
    aws_lambda_event_sources,
    aws_dynamodb,
    aws_iam,
    aws_sqs,
    aws_logs,
    aws_events,
    aws_events_targets


)
class AwsResource():
    pass #future parameters

class MyRealityStack(core.Stack):

    def __init__(
            self,
            scope: core.Construct,
            construct_id: str,
            env:core.Environment,
            **kwargs,
             ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # AWS Resource Declaration - just to ensure the scope
        #DynamoDB Table
        self.ddb_table_reality = None
        #DynamoDB Streams Event Source 
        self.ddb_streams_reality = None
        #Lambdas
        self.lambda_FetchReality = None  #main function for API call
        self.lambda_RealityDBTrigger = None #process dynamoDB stream
        self.lambda_Poll_SQS_SendEmail= None # self explanatory :D
        # SQS Queues
        self.queue_ddb_streams = None 
        ##TO BE##
        # REST/ graph API 
        # Create CDK resources 
        self.create_cdk_resources()
    
    def create_cdk_resources(self) -> None:
        self.create_queues()
        self.create_dynamodb()
        self.create_lambdas()
        self.grant_dynamodb_permissions()
        #self.create_rest_api()
    
    def create_queues(self) -> None:
        '''SQS Queues
        '''
        self.queue_ddb_streams = aws_sqs.Queue(
            self,
            'DynamoChangesQueue-dev',
            retention_period=core.Duration.days(5),  # Max supported by SQS 14days
            visibility_timeout=core.Duration.minutes(10),  # Max Lambda timeout 15
        )
    def create_dynamodb(self) -> None:
        '''DynamoDB Tables and Streams Event Sources
        '''
        # Single-table to store Reality #possibility to change billing,ttl here, global index in new function
        self.ddb_table_reality = aws_dynamodb.Table(
            self,
            'Reality-dev',
            partition_key= aws_dynamodb.Attribute(name="hash_id", type=aws_dynamodb.AttributeType.NUMBER),
            sort_key=aws_dynamodb.Attribute(name='actualized',type=aws_dynamodb.AttributeType.STRING),
            stream= aws_dynamodb.StreamViewType.NEW_AND_OLD_IMAGES, #enable dynamo streams for the trigger
            read_capacity=3,
            write_capacity=8,
            removal_policy=core.RemovalPolicy.DESTROY # hidden in core
        )
        ## Streams DB + triggers
        self.ddb_streams_reality = aws_lambda_event_sources.DynamoEventSource(
            table=self.ddb_table_reality,
            starting_position=aws_lambda.StartingPosition.LATEST,# check what trim horizon means
            batch_size=100,  # check is 5 enough? is 500 a lot 
            max_batching_window= core.Duration.seconds(60),
            #parallelization_factor = ?2 ?5 
            retry_attempts=2,
            on_failure=aws_lambda_destinations.SqsDestination(
                self.queue_ddb_streams),

        )
            
    def create_lambdas(self) -> None:
        '''Lambda Functions
        '''
        self.lambda_FetchReality = aws_lambda.Function(
            self,
            'FetchReality-dev',
            runtime=aws_lambda.Runtime.PYTHON_3_8,
            code=aws_lambda.Code.asset('lambda/FetchReality'), #relative path to main folder eg. lambda/extra_function
            handler='FetchReality.handler',
            memory_size=128,
            timeout=core.Duration.minutes(10),#long running on full db
            log_retention=aws_logs.RetentionDays.ONE_MONTH,
            #reserved_concurrent_executions= check what this is doing
            #events=aws_events.Schedule.cron(day=None,hour=7),.
            environment={'env':'dev'}
        )
        #self.rule = aws_events.Rule(self,'scheduleFetchReality',
            #enabled=True,
            #schedule= aws_events.Schedule.cron(hour="7"),
            #targets= aws_events_targets.LambdaFunction(handler=self.lambda_FetchReality))
        self.lambda_Poll_SQS_SendEmail = aws_lambda.Function(
            self,
            'Poll_SQS_SendEmail-dev',
            runtime=aws_lambda.Runtime.PYTHON_3_8,
            code=aws_lambda.Code.asset('lambda/Poll_SQS_SendEmail'), 
            handler='Poll_SQS_SendEmail.handler',
            memory_size=128,
            timeout=core.Duration.seconds(60),
            log_retention=aws_logs.RetentionDays.ONE_WEEK,
            #reserved_concurrent_executions= check what this is doing
            #events=[self.], #what is triggering this one
            
            environment={'env':'dev'}
        )

        self.lambda_RealityDBTrigger = aws_lambda.Function(
            self,
            'RealityDBTrigger-dev',
            runtime=aws_lambda.Runtime.PYTHON_3_8,
            code=aws_lambda.Code.asset('lambda/RealityDBTrigger'), 
            handler='RealityDBTrigger.handler',
            memory_size=128,
            timeout=core.Duration.seconds(40),
            log_retention=aws_logs.RetentionDays.ONE_MONTH,
            #reserved_concurrent_executions= check what this is doing
            events=[self.ddb_streams_reality], #what is triggering this one
            environment={'env':'dev'},
            on_success=aws_lambda_destinations.LambdaDestination(self.lambda_Poll_SQS_SendEmail,response_only=True)
            
        )

    def grant_dynamodb_permissions(self) -> None:
        '''Grant permissions to interact with DynamoDB Resources
        '''
        self.ddb_table_reality.grant_read_write_data(self.lambda_FetchReality)

       

        ## Example automatically generated without compilation. See https://github.com/aws/jsii/issues/826
        #table = Table.from_table_arn(self, "ImportedTable", "arn:aws:dynamodb:us-east-1:111111111:table/my-table")
        # now you can just call methods on the table
        ##table.grant_read_write_data(user)