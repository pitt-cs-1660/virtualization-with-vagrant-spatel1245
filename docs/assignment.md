# Assignment

After we set up our Vagrantfile we are ready to create our development environment. The application code is in [app.py](../app.py) and it stands up a simple
web server using the `Flask` framework. The service is a lightweight pixel tracker that stores ip and device data. The web server exposes an endpoint (`/pixel`)
and persists data to the mysql database. For this assignment we are responsible for creating our mysql database and data table.

You will need to update the çç with mysql commands that create a database called cs1660, and a table called pixel_data. 
This assignment is not meant to test you mysql knowledge so those commands are proved below.

```shell
mysql -u root -proot -e "CREATE DATABASE cs1660 ;"
mysql -u root -proot cs1660 -e "
CREATE TABLE pixel_data (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, 
    date DATE not null, 
    useragent VARCHAR(255), 
    ip VARCHAR(255) NOT NULL, 
    thirdpartyid VARCHAR(255)
) ;"
```

If you inspect the mysql commands above you will notice a few things.
1. The commands are being run in a script so we are the [-e or --execute](https://dev.mysql.com/doc/refman/5.7/en/mysql-command-options.html#option_mysql_execute) flag
2. The root password is in plain text (do not do that in the real world)
3. The SQL commands are wrapped in `"` and ended with `;`
4. Line 18 on the [init.sh](../bootstrap/init.sh) script creates the vagrant user in our database and sets a password (which is...password)

You will need to use the `-e` or `--exectute` flags to run you sql commands in the script. The `create table` command needs to be executed against the `cs1660` database so please be mindful when copying that example above.

## Verify Database Table
Assuming init script is set up correctly you can connect to your VM and verify that the database and table are working correctly. Please see the commands below

```shell
# connect to the guest os
vagrant ssh

# connect to mysql 
mysql>mysql -p
Enter password: 

# show database
mysql>show databases;

# connect to database
mysql>connect cs1660;

# show tables
mysql>show tables;
```

## Start Server
Now that the database is configured we can start our web server. We do not need to worry about installing dependencies because that was handled in the [init.sh](../bootstrap/init.sh).

```shell
# connect to your machine
vagrant ssh

# navigate to the /vagrant directory
cd /vagrant

# start the web sever
python3 app.py
```

## Test The Server
In a separate from you local machine you can test the server with [cURL](https://curl.se/). We configured our VM with `forwarded_port` setting that defines
which ports are mapped from our local to Guest OS. In our case port 5000 is exposed on the Guest OS. We are defining the port on line 24 of the [app.py](../app.py) with a default vaule
of 5000.

Run the following `cURL` and verify that you are getting a 200 response back.

```shell
curl -XPOST \
  -H "Content-Type: application/json" \
  http://localhost:5000/pixel \
  -d '{"date":"2023-08-07 10:22:03","ip":"64.58.244.138","useragent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36","thirdpartyid":"www.someplacecool.run"}'
```

You can also check the data in the table directly.

```shell
vagrant ssh 

mysql -p cs1660
Enter password:

mysql>select * from pixel_data;
```
