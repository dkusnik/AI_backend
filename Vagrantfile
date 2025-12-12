# -*- mode: ruby -*-
# vi: set ft=ruby :

VAGRANTFILE_API_VERSION = "2"

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
  config.vm.define "debian-k8s" do |k8s|
    k8s.vm.box = "bento/debian-13"

    k8s.vm.provider "virtualbox" do |vb|
      vb.memory = 12000
      vb.cpus = 4
      vb.gui = false
    end

    # Use a private network (adjust IP as needed)
    k8s.vm.network "private_network", ip: "192.168.50.4"

    # Mount the repo root as /browsertrix (not /vagrant)
    config.vm.synced_folder ".", "/AI_backend"

    # Provision using external script
    k8s.vm.provision "shell", path: "tools/vagrant/bootstrap.sh"
  end
end