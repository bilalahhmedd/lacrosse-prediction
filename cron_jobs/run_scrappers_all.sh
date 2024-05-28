source /home/$USER/envs/lacrosse_prediction/bin/activate
PARENT_DIR="$(dirname "$(dirname "$(realpath "$0")")")"
cd $PARENT_DIR
CRON_JOBS_DIR=$PARENT_DIR/cron_jobs
echo $CRON_JOBS_DIR
bash $CRON_JOBS_DIR/commitments_scrappers.sh
bash $CRON_JOBS_DIR/camps_and_clinics_scrappers.sh
bash $CRON_JOBS_DIR/clubs_ranking_scrappers.sh
bash $CRON_JOBS_DIR/colleges_ranking_scrappers.sh
bash $CRON_JOBS_DIR/schools_ranking_scrappers.sh
