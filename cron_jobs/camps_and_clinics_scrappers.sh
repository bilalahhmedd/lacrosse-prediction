source /home/$USER/envs/lacrosse_prediction/bin/activate
PARENT_DIR="$(dirname "$(dirname "$(realpath "$0")")")"
cd $PARENT_DIR
SCRAPPER_DIR=$PARENT_DIR/scrappers
echo "running camps and clinics airtable scrapper"
python  $SCRAPPER_DIR/camps_and_clinics_airtable_scrapper.py
echo "running camps and clinics mass elite scrapper"
python  $SCRAPPER_DIR/camps_and_clinics_mass_elite_scrapper.py
