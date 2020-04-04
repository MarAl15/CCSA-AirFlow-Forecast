# Container image
FROM python:3.6-slim-stretch

# Just add the required files
ARG VERSION # Build time
ENV VERSION $VERSION # Runtime
ADD API$VERSION.py requirements.txt ./workflow/

# Set working directory
WORKDIR ./workflow

# Create folder for the saved models (pickle files) and install software packages
RUN mkdir ~/.models && apt-get update && pip install -r requirements.txt

# Inform Docker that the container listens at port $PORT at runtime
ENV PORT $PORT
EXPOSE $PORT

# Deploy the Restful API app
CMD gunicorn -w 3 -b 0.0.0.0:$PORT API$VERSION:app
