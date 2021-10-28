if [ ! $# -eq 1 ]
  then
      echo "./$0 <crawl_id>"
      exit
fi

crawl_id="$1"


project_folder=$(python -m crawler.configuration crawler project_folder)

#temp_folder="/mnt/xvdf/tmp/${crawl_id}/"
temp_folder=$project_folder"/tmp/"
mkdir -p "$temp_folder"
cd "$temp_folder"

for zf in $project_folder/data/"${crawl_id}"/*.zip; do
    rm -r raw_html
    tf=${zf%.zip}.tgz
    unzip "$zf" && tar -zcf "$tf" "raw_html" && touch -r "$zf" "$tf" && rm "$zf"
done
