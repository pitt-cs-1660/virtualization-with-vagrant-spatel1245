#!/usr/bin/env bash

# update
apt-get update -y

# install mysql, python, and other stuff
apt-get install -y vim curl wget mysql-server git tree python3-dev python3-pip python3-venv apt-transport-https
apt-get upgrade python3 -y
apt-get -y autoremove -y

# Allow connections form outside the vm, so comment out the bind-address in the appropriate file, depending on which version of mysql we have...
# This is for older versions...
sed -i "s/^bind-address/#bind-address/" /etc/mysql/my.cnf
# This is for newer versions...
sed -i "s/^bind-address/#bind-address/" /etc/mysql/mysql.conf.d/mysqld.cnf
mysql -u root -proot -e "GRANT ALL PRIVILEGES ON *.* TO 'root'@'localhost'; FLUSH PRIVILEGES;
SET GLOBAL max_connect_errors=10000;"
# create vagrant user and set a password
mysql -u root -proot -e "CREATE USER 'vagrant'@'localhost' IDENTIFIED BY 'password'; GRANT ALL PRIVILEGES ON *.* TO 'vagrant'@'localhost'; FLUSH PRIVILEGES;"

############# DO NOT MODIFY ANYTHING ABOVE OR BELOW THIS COMMENT SECTION #############
# create mysql resources here

######################################################################################
 systemctl restart mysql

# Create a Python3 Virtual Environment and Activate it in .profile
sudo -H -u vagrant sh -c 'python3 -m venv /vagrant/.venv'
sudo -H -u vagrant sh -c 'echo ". /vagrant/.venv/bin/activate" >> ~/.profile'

# Install app dependencies in virtual environment as vagrant user
sudo -H -u vagrant sh -c '. /vagrant/.venv/bin/activate && pip install -U pip && pip install wheel'
sudo -H -u vagrant sh -c '. /vagrant/.venv/bin/activate && cd /vagrant && pip install -r requirements.txt'