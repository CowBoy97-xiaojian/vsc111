[app@application-1 cdp-deploy-4.1]$ cat /data1/deploy/cdp-deploy-4.1/play_all.yml
---
- hosts: localhost
  tasks:
    - name: set python version
      set_fact:
        ansible_python_interpreter:  "/usr/bin/python"

    - name: Install sshpass
      yum:
        name: sshpass
        state: present
      become: yes
      when: ansible_distribution == 'CentOS' or ansible_distribution == 'Red Hat Enterprise Linux'

    - name: Generate an OpenSSH keypair
      openssh_keypair:
        path: '~/.ssh/id_rsa'
        size: 2048
        type: rsa
      run_once: true

- hosts: all
  tasks:
    - name: Add authorized_key for user root
      authorized_key:
        user: root
        state: present
        key: "{{ lookup('file', '~/.ssh/id_rsa.pub') }}"
      become: yes

    - name: Check OS Distribution
      assert:
        that:
          - ansible_os_family == 'RedHat'
          - ansible_machine == 'x86_64'
          - ansible_distribution_version >= '7.2'
        success_msg: 'Distribution is OK'
        quiet: yes
      when: ansible_distribution == 'CentOS'

    - name: Check Sudo Permisson
      command: ls /tmp
      become: yes

    - name: Disable Selinux
      selinux: state=disabled

    - name: Test disk fs format is Ext4 or XFS
      assert:
        that: item.fstype == 'xfs' or item.fstype == 'ext4'
        success_msg: Disk Fs Format OK
        quiet: yes
      with_items: "{{ ansible_mounts }}"
      loop_control:
        label: "{{ item.fstype }}"

    - name: Check Memory Size
      assert:
        that:
          - ansible_memtotal_mb >= {{ memory * 900 }}

    - name: Check CPU vCores
      assert:
        that:
          - ansible_processor_vcpus >=  {{ cpu }}

    - name: Set Hostname
      hostname:
        name: "{{ hostname }}"

- hosts: application
  tasks:
    - name: Check required mount points for application nodes
      fail:
        msg: "{{ item.mount }} must be a mount point"
      when: ansible_mounts | selectattr('mount', 'equalto', item.mount) | list | length == 0
      with_items: "{{ applicationMounts }}"
  vars_files:
    - disk.yml

- hosts: store
  tasks:
    - name: Check required mount points for store nodes
      fail:
        msg: "{{ item.mount }} must be a mount point"
      when: ansible_mounts | selectattr('mount', 'equalto', item.mount) | list | length == 0
      with_items: "{{ storeMounts }}"
  vars_files:
    - disk.yml

- hosts: collector
  tasks:
    - name: Check required mount points for collector nodes
      fail:
        msg: "{{ item.mount }} must be a mount point"
      when: ansible_mounts | selectattr('mount', 'equalto', item.mount) | list | length == 0
      with_items: "{{ collectorMounts }}"
  vars_files:
    - disk.yml

- hosts: compute
  tasks:
    - name: Check required mount points for compute nodes
      fail:
        msg: "{{ item.mount }} must be a mount point"
      when: ansible_mounts | selectattr('mount', 'equalto', item.mount) | list | length == 0
      with_items: "{{ computeMounts }}"
  vars_files:
    - disk.yml

- hosts: metabase
  tasks:
    - name: Check required mount points for metabase nodes
      fail:
        msg: "{{ item.mount }} must be a mount point"
      when: ansible_mounts | selectattr('mount', 'equalto', item.mount) | list | length == 0
      with_items: "{{ metabaseMounts }}"
  vars_files:
    - disk.yml

- hosts: simba
  tasks:
    - name: Check required mount points for simba nodes
      fail:
        msg: "{{ item.mount }} must be a mount point"
      when: ansible_mounts | selectattr('mount', 'equalto', item.mount) | list | length == 0
      with_items: "{{ simbaMounts }}"
  vars_files:
    - disk.yml

- name: generate /etc/hosts file
  hosts: all
  roles:
    - hosts