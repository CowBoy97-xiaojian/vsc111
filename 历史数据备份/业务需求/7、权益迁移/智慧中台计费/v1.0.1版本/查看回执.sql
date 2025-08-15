
02912
lftp -u sftp_coc270,SFtp_cOc270! -p 3964 sftp://10.252.180.2/outgoing/EC-DATA/029/02912

lftp -u sftp_coc270,SFtp_cOc270! -p 3964 sftp://10.252.180.2/outgoing/EC-DATA/029/477670

lftp -u sftp_coc270,SFtp_cOc270! -p 3964 sftp://10.252.180.2/incoming/EC-DATA/029/ -e "mput -e *.txt.gz;mput -e *.chk; exit"