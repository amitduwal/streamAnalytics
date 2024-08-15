FROM public.ecr.aws/lambda/python:3.10

COPY requirements.txt ./

RUN pip install -r requirements.txt

COPY lambda.py ./

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "lambda.lambda_handler" ]

