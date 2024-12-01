#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
static const int MAX_ID_LENGTH = 256;
static const int MAX_NAME_LENGTH = 256;

// Define struct types for patient and doctor
typedef struct {
    char patient_id[MAX_ID_LENGTH];
    char name[MAX_NAME_LENGTH];
    char city[MAX_NAME_LENGTH];
    char state[MAX_NAME_LENGTH];
} Patient;

typedef struct {
    char patient_id[MAX_ID_LENGTH];
    char doctor_name[MAX_NAME_LENGTH];
} Doctor;

// Define the struct type for the a single record in the merged result
typedef struct {
    char patient_id[MAX_ID_LENGTH];
    char name[MAX_NAME_LENGTH];
    char city[MAX_NAME_LENGTH];
    char state[MAX_NAME_LENGTH];
    char doctor_name[MAX_NAME_LENGTH];
} MergedRecord;

// define the struct type for argument for the nested loop join/merge join function
typedef struct {
    MergedRecord *my_result;
    int my_result_count;
    int my_rank;
    int my_n_count;
} ThreadArg;

// initialize global variables for patients list, doctors list, final merged result list, and their respective counts
Patient *patients = NULL;
Doctor *doctors = NULL;
MergedRecord *result = NULL;
int* result_count;
int patient_count;
int doctor_count;

// Read from R.csv and save to patients array
int read_from_r(const char *filename, Patient **patients) {
    FILE *file = fopen(filename, "r");

    char line[256];
    int counter = 0;

    // Skip header
    fgets(line, sizeof(line), file);

    while (fgets(line, sizeof(line), file)) {
        *patients = realloc(*patients, (counter + 1) * sizeof(Patient));
        sscanf(line, "%[^,],%[^,],%[^,],%s",
               (*patients)[counter].patient_id,
               (*patients)[counter].name,
               (*patients)[counter].city,
               (*patients)[counter].state);
        counter++;
    }

    fclose(file);
    return counter;
}

// Read from S.csv and save to doctors array
int read_from_s(const char *filename, Doctor **doctors) {
    FILE *file = fopen(filename, "r");

    char line[256];
    int counter = 0;

    // Skip header
    fgets(line, sizeof(line), file);

    while (fgets(line, sizeof(line), file)) {
        *doctors = realloc(*doctors, (counter + 1) * sizeof(Doctor));
        if (!(*doctors)) {
            perror("Memory allocation failed for doctors");
            fclose(file);
            return -1;
        }

        sscanf(line, "%[^,],%s", (*doctors)[counter].patient_id, (*doctors)[counter].doctor_name);
        counter++;
    }

    fclose(file);
    return counter;
}

// Write merged records to a CSV file
void write_to_CSV(const char *filename, MergedRecord *records, int count) {
    FILE *file = fopen(filename, "w");

    // Write CSV header
    fprintf(file, "patient_id,name,city,state,doctor\n");
    printf("The id is: %s\n", records[0].patient_id);
    for (int i = 0; i < count; i++) {
        fprintf(file, "%s,%s,%s,%s,%s\n",
                records[i].patient_id,
                records[i].name,
                records[i].city,
                records[i].state,
                records[i].doctor_name);
    }

    fclose(file);
    printf("Merged records written to %s successfully.\n", filename);
}
//nested loop join
void *nested_loop_join(void *args) {

    ThreadArg *my_args = (ThreadArg *)args;
    int my_n = my_args->my_n_count;
    int my_rank = my_args->my_rank;
    int my_i_start = my_n * my_rank;
    int my_i_end = my_i_start + my_n;
    int k = 0;
    for(int i = my_i_start; i< my_i_end; i++){
        for(int j = 0; j < doctor_count; j++){
            int cmp = atoi(patients[i].patient_id) - atoi(doctors[j].patient_id);

            if (cmp == 0) {
                strcpy(my_args->my_result[k].patient_id, patients[i].patient_id);
                strcpy(my_args->my_result[k].name, patients[i].name);
                strcpy(my_args->my_result[k].city, patients[i].city);
                strcpy(my_args->my_result[k].state, patients[i].state);
                strcpy(my_args->my_result[k].doctor_name, doctors[j].doctor_name);
                k++;
            }
            else {
                continue;
            }
        }
    }

        
    result_count[my_rank] = k;
    pthread_exit(NULL);
}

// Merge join function
void *merge_join(void *args) {

    ThreadArg *my_args = (ThreadArg *)args;
    int my_n = my_args->my_n_count;
    int my_rank = my_args->my_rank;
    int my_i_start = my_n * my_rank;
    int my_i_end = my_i_start + my_n;
    int i = my_i_start;
    int j = 0;
    int k = 0;

    while (i < my_i_end && j < doctor_count) {
        int cmp = atoi(patients[i].patient_id) - atoi(doctors[j].patient_id);

        if (cmp == 0) {
            strcpy(my_args->my_result[k].patient_id, patients[i].patient_id);
            strcpy(my_args->my_result[k].name, patients[i].name);
            strcpy(my_args->my_result[k].city, patients[i].city);
            strcpy(my_args->my_result[k].state, patients[i].state);
            strcpy(my_args->my_result[k].doctor_name, doctors[j].doctor_name);
            k++;
            i++;
            j++;
        } else if (cmp < 0) {
            i++;
        } else {
            j++;
        }
    }
        
    result_count[my_rank] = k;
    pthread_exit(NULL);
}

// Comparison functions for qsort
int compare_for_patient(const void* a, const void* b)
{
    return atoi(((Patient*)a)->patient_id)
           - atoi(((Patient*)b)->patient_id);
}
// Qsort comparison function for doctor
int compare_for_doctor(const void* a, const void* b)
{
    return atoi(((Doctor*)a)->patient_id)
           - atoi(((Doctor*)b)->patient_id);
}


int main(int argc, char* argv[]) {
    // read algorithm name, thread counta, and tables name from command line
    char *alg_name = argv[1];
    int thread_count = atoi(argv[2]);
    char *r_file_name = argv[3];
    char *s_file_name = argv[4];
    if (thread_count <= 0) {
        fprintf(stderr, "Invalid number of threads: %d\n", thread_count);
        return -1;
    }

    pthread_t* thread_handles = malloc(thread_count * sizeof(pthread_t));
    // Initialize to result_count to zeros
    result_count = calloc(thread_count, sizeof(int));
    // read from R and S table, and update patients, doctors, patient count and doctor count
    patient_count = read_from_r(r_file_name, &patients);
    doctor_count = read_from_s(s_file_name, &doctors);
    
    // sort patients and and doctors from by patient_id
    qsort(patients, patient_count, sizeof(Patient), compare_for_patient);
    qsort(doctors, doctor_count, sizeof(Doctor), compare_for_doctor);
    
    // calculate the size of assigned portion for each thread
    int my_n = patient_count / thread_count;
    
    // initialize array to store different threads sorted by each thread
    MergedRecord* thread_results[thread_count];
    
    // thread_args store the arguments to be passed to merge join by each thread
    ThreadArg thread_args[thread_count];
    // initialize thread_count number of threads and let each thread call merge join
    for (int thread = 0; thread < thread_count; thread++) {
//        int my_i_start = my_n * thread;
//        int my_actual_n = (thread == thread_count - 1) ? (patient_count - my_i_start) : my_n;

        thread_results[thread] = malloc(my_n * sizeof(MergedRecord));
        thread_args[thread].my_result = thread_results[thread];
        thread_args[thread].my_rank = thread;
        thread_args[thread].my_n_count = my_n;
        // call either merge join or nested loop join depending on the input, and in case threads cannot be initialized, output an error message
        if (strcmp(alg_name, "pmj") == 0) {
            if (pthread_create(&thread_handles[thread], NULL, merge_join, &thread_args[thread]) != 0) {
                perror("Thread creation failed");
                exit(1);
            }
        }
        else if (strcmp(alg_name, "pnlj") == 0){
            if (pthread_create(&thread_handles[thread], NULL, nested_loop_join, &thread_args[thread]) != 0) {
                perror("Thread creation failed");
                exit(1);
            }
        }
    }
    // terminates threads
    for (int thread = 0; thread < thread_count; thread++) {
        pthread_join(thread_handles[thread], NULL);
    }
    // calculates number of
    int totalResults = 0;
    for (int thread = 0; thread < thread_count; thread++) {
        totalResults += result_count[thread];
    }
    
    result = malloc(totalResults * sizeof(MergedRecord));
    // assign the value for final result by copying the memory block from each thread using memcpy function
    int offset = 0;
    for (int thread = 0; thread < thread_count; thread++) {
        printf("The id is: %s\n", thread_results[thread]->patient_id);
        memcpy(result + offset, thread_results[thread], result_count[thread] * sizeof(MergedRecord));
        offset += result_count[thread];
        free(thread_results[thread]);
    }


    write_to_CSV("merged_records_parallel.csv", result, totalResults);
    // free dynamically allocated memories
    free(patients);
    free(doctors);
    free(result);
    free(thread_handles);
    free(result_count);

    return 0;
}
