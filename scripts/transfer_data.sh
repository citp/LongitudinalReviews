if [ ! $# -eq 1 ]
  then
      echo "./$0 <crawl_id>"
      exit
fi

crawl_id="$1"

remote=$(python -m crawler.configuration analysis_machine user)"@"$(python -m crawler.configuration analysis_machine hostname)
project_folder=$(python -m crawler.configuration crawler project_folder)
remote_pf=$(python -m crawler.configuration analysis_machine project_folder)


rsync -uavz --exclude="raw_html" --exclude="*.bak" --exclude="*.tgz" --exclude="*.tgz" --exclude="*.zip" $project_folder/data/"${crawl_id}" "${remote}:${remote_pf}/data/"
