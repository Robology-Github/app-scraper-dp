# Use an official Node.js runtime as a parent image
FROM node:21.7.1

# Set the working directory in the container to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . .

# Install dependencies required to compile OpenSSL
RUN apt-get update && \
    apt-get install -y build-essential checkinstall zlib1g-dev wget

# Download and compile OpenSSL from source
RUN wget https://www.openssl.org/source/openssl-3.2.1.tar.gz \
    && tar -xzvf openssl-3.2.1.tar.gz \
    && cd openssl-3.2.1 \
    && ./config --prefix=/usr/local/ssl --openssldir=/usr/local/ssl shared zlib \
    && make \
    && make test \
    && make install

# Configure the shared libraries and ensure the new OpenSSL binaries are in the PATH
RUN echo "/usr/local/ssl/lib" > /etc/ld.so.conf.d/openssl-3.2.1.conf \
    && ldconfig -v \
    && mv /usr/bin/openssl /usr/bin/openssl.backup \
    && ln -s /usr/local/ssl/bin/openssl /usr/bin/openssl

# Clean up the apt cache and temporary files
RUN apt-get clean && rm -rf /var/lib/apt/lists/* /openssl-3.2.1.tar.gz /openssl-3.2.1

# Install Python dependencies from requirements.txt
# Ensure you have a requirements.txt file in your project directory
COPY requirements.txt /app/
RUN pip3 install --no-cache-dir -r requirements.txt

# Download necessary NLTK data
RUN python3 -m nltk.downloader punkt stopwords
# Run TextBlob's download script
RUN python3 -m textblob.download_corpora lite

# Install any needed packages specified in package.json for Node.js
RUN npm install

# Run your application
CMD ["npm", "start"]
