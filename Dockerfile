# Use an official Node.js runtime as a parent image
FROM node:latest

# Set the working directory in the container to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . .

# Install any needed packages specified in requirements.txt for Python
# First, ensure python3 and pip are installed
RUN apt-get update && \
    apt-get install -y python3 python3-pip



# Install Python dependencies from requirements.txt
# Note: Ensure you have a requirements.txt file in your project directory
COPY requirements.txt /app/
RUN pip3 install --no-cache-dir -r requirements.txt

# Install any needed packages specified in package.json for Node.js
RUN npm install




# Run app.py when the container launches
# Note: Replace "npm start" with your start command if different
CMD ["npm", "start"]