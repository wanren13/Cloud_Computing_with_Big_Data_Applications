data = LOAD '$INPUT';
subset = SAMPLE data $PERCENT;
STORE subset INTO '$OUTPUT';