# CS 1660 Cloud Computing Vagrant Lab
This lab assignment will show you how to use Vagrant, and VirtualBox to automate a development environment. In this project we are creating small pixel tracking application that consists of a python (Flask) web server that connects to a mysql database. We will be using [Vagrant](https://developer.hashicorp.com/vagrant/docs) to provision a virtual machine using Virtual Box.

Vagrant provides a great development cycle, and standardized virtual machine usage for software engineers.

## Prerequisites
- [Vagrant](https://developer.hashicorp.com/vagrant/docs/installation)
- [VirtualBox](https://www.virtualbox.org/wiki/Downloads)

### Note Apple M1/2 Chip Owners
If you have the new Macbook Pros with M1/2 chips you need to get your download [HERE](https://download.virtualbox.org/virtualbox/7.0.0_BETA3/) (select `macOSAArch64.dmg` file). This version is in BETA, but should work for 
this assignment

## Setup
```shell
# clone the repository
git clone git@github.com:dansc0de/cc-vagrant.git
cd cc-vagrant/
```

## Updates To Vagrantfile
The following changes will bootstrap our environment so we can get the service running. We will be using [Ubuntu Jammy Jelly](https://releases.ubuntu.com/jammy/) as our guest OS. 

Please note that the configurations below should appear in the `Vagrantfile.configure` block. We will be uncommenting all of our settings within the block. You should not need 
to create new settings!
```ruby
Vagrant.configure("2") do |config|
  << YOUR CONFIGS HERE >>
end
```

### Set Guest OS and Hostname 

```ruby
# Every Vagrant development environment requires a box. You can search for
# boxes at https://vagrantcloud.com/search.
config.vm.box = "ubuntu/jammy64"

# Set vm hostname
config.vm.hostname = "pittcs1660"
```

### Configure Port Forward

Our Python Flask apps listens on port `5000` by default so we need to forward the port from
inside the VM to our workstation so that we can access it with our curl commands.

```ruby
config.vm.network "forwarded_port", guest: 5000, host: 5000, host_ip: "127.0.0.1"
```

### Configure Sync Local Directory
Share an additional folder to the guest VM. The first argument is the path on the host to the actual folder. 
The second argument is the path on the guest to mount the folder. And the optional third argument is a set of non-required options.
Please note the file path here `/src/app`
```ruby
config.vm.synced_folder ".", "/src/app/", :mount_options => ["dmode=777", "fmode=666"]
```

### Configure Provider
The provider block determines what virtualization tool will provision the Guest OS. We are using VirtualBox in this assignment. Within this block 
we are setting resource limits and DNS settings. 

```ruby
config.vm.provider "virtualbox" do |vb|
    vb.customize [ "modifyvm", :id, "--uartmode1", "disconnected" ]
    vb.memory = 1024
    vb.cpus = 4
    # Fixes some DNS issues on some networks
    vb.customize ["modifyvm", :id, "--natdnshostresolver1", "on"]
    vb.customize ["modifyvm", :id, "--natdnsproxy1", "on"]
end
```

### Install Dependencies
We have a bootstrap [script](./bootstrap/init.sh) that installs mysql server, python3.10, and python dependencies. 
We are using the VM provision utility to run the bootstrap script at creation time of the VM. Vagrant will run this script 
from the `/tmp` directory.

```ruby
config.vm.provision "shell", :path => "./bootstrap/init.sh"
```

NOTE: You can also use the `vm.provision` utility inline like this...

```ruby
config.vm.provision "shell", inline: <<-SHELL
    # Update and install something...
    apt-get update
    ...    
SHELL
```

### Vagrant up!
We are now ready to start our VM. Run `vagrant up` from the root of this project and wait...

Once the VM is running you can connect with the `vagrant ssh` command. Now we can proceed to the [assignment](./docs/assignment.md).
