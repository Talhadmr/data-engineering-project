#!/bin/bash

# Get OS version
os_version=$(lsb_release -rs)

# Check for Docker
if ! command -v docker &> /dev/null
then
    echo "Docker is not installed. Installing Docker..."
    
    # Install Docker
    sudo apt-get update
    sudo apt-get install -y \
        ca-certificates \
        curl \
        gnupg \
        lsb-release

    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg

    echo \
      "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu \
      $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

    sudo apt-get update
    sudo apt-get install -y docker-ce docker-ce-cli containerd.io

    # Start Docker and add user to docker group
    sudo systemctl start docker
    sudo usermod -aG docker $USER

    echo "Docker installed successfully."
else
    echo "Docker is already installed."
fi

# Check for Docker Compose
if ! command -v docker-compose &> /dev/null
then
    echo "Docker Compose is not installed. Installing Docker Compose..."

    # Get the latest version of Docker Compose
    compose_version=$(curl -s https://api.github.com/repos/docker/compose/releases/latest | grep tag_name | cut -d '"' -f 4)

    # Install Docker Compose
    sudo curl -L "https://github.com/docker/compose/releases/download/$compose_version/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
    sudo chmod +x /usr/local/bin/docker-compose

    # Create a symbolic link for Docker Compose
    sudo ln -s /usr/local/bin/docker-compose /usr/bin/docker-compose

    echo "Docker Compose installed successfully."
else
    echo "Docker Compose is already installed."
fi