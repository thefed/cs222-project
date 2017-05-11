# this script configures the log file name, test script and project directory of a project

# it will run all test cases and save the output to log file specified

# to test a specified project, uncomment the its variables
clear   # clear screen

# if a parameter is given as {1,2,3,4}, the specified project test script will run
#$1    # project id
#if [ $# -eq 1 ]; then 
#    LOG_FILE='project'$1'.log'
#    TEST_SCRIPT='test-p'$1'.sh'
#    PROJECT_DIR='project'$1
#else    # test project 1 by default

# project 1
LOG_FILE='project1.log'
TEST_SCRIPT='test-p1.sh'
PROJECT_DIR='project1'

# project 2
#LOG_FILE='project2.log'
#TEST_SCRIPT='test-p2.sh'
#PROJECT_DIR='project2'

# project 3
#LOG_FILE='project3.log'
#TEST_SCRIPT='test-p3.sh'
#PROJECT_DIR='project3'

# project 4
#LOG_FILE='project4.log'
#TEST_SCRIPT='test-p4.sh'
#PROJECT_DIR='project4'

#fi

#./test-p4.sh project4 | tee $LOG_FILE

printf "*** output will be saved to log file %s ***\n" $LOG_FILE

# redirect stdout and stderr to log file
./$TEST_SCRIPT $PROJECT_DIR 2>&1 | tee $LOG_FILE
echo `date` >> $LOG_FILE

# save a copy of log file in project folder (already saved)
#cp $LOG_FILE $PROJECT_DIR

echo "**** Tests end here ****"
