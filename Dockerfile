# Start from a base image that includes Node.js
FROM node:14

# Install Python 3
RUN apt-get update && apt-get install -y python3 python3-pip

# Install Python libraries
RUN pip3 install pandas textblob re json argparse

# Copy your application code (both Node.js and Python scripts)
WORKDIR /app
COPY . /app

# Install your Node.js dependencies
RUN npm install

# Your app's start command
CMD ["npm", "start"]

