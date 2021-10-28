if [ ! $# -eq 1 ]
  then
      echo "./$0 <crawl_id>"
      exit
fi


crawl_id="$1"

backup_drive=$(python -m crawler.configuration crawler backup_drive)
proj_folder=$(python -m crawler.configuration crawler project_folder)
backup_folder=/mnt/$backup_drive/$(python -m crawler.configuration crawler backup_drive_folder)

sudo mkdir /mnt/$backup_drive
sudo chown ubuntu:ubuntu /mnt/$backup_drive
sudo mount /dev/xvdf /mnt/$backup_drive
rsync -uav $proj_folder/logs/"${crawl_id}" $backup_folder/logs/
rsync -uav --exclude="*.bak" $proj_folder/data/"${crawl_id}" $backup_folder/data/
