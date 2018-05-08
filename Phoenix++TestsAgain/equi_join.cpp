// String Match application scrolls through a list of keys (provided in a file)
// in order to determine if any of them occur in a list of encrypted words (which 
// are hardcoded into the application).


#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <fstream>
#include <sstream>
#include <vector>
#include <sys/time.h>
#include <time.h>
#include <iostream>

using namespace std;

#include "map_reduce.h"

#define DEFAULT_UNIT_SIZE 5
#define SALT_SIZE 2
#define MAX_REC_LEN 1024
#define OFFSET 5


#define MUST_REDUCE

struct wc_string {
    char *data_A, *data_B, *data_A_key, *data_B_key;
    uint64_t lenA, lenB;
};

struct wc_word {

    char* data;
    
    //necessary functions to use this as a key
    bool operator<(wc_word const& other) const {
        return strcmp(data, other.data) < 0;
    }
    bool operator==(wc_word const& other) const {
        return strcmp(data, other.data) == 0;
    }
};

struct valueStruct {

    int identifier;
    char* value_data;

    valueStruct() { identifier = -1; value_data = NULL;}
    valueStruct(int identifier, char* value_data) {  this->identifier = identifier; this->value_data = value_data; }
};


struct wc_word_hash
{
    // FNV-1a hash for 64 bits
    size_t operator()(wc_word const& key) const
    {
        char* h = key.data;
        uint64_t v = 14695981039346656037ULL;
        while (*h != 0)
            v = (v ^ (size_t)(*(h++))) * 1099511628211ULL;
        return v;
    }
};


class JoinMR : public MapReduce<JoinMR, wc_string, wc_word, valueStruct, hash_container<wc_word, valueStruct, buffer_combiner, wc_word_hash> >
{
    char *data_A, *data_B, *data_A_key, *data_B_key;
    uint64_t lenA, lenB;
    uint64_t splitter_pos_A, splitter_pos_B, chunk_size;
    int key_column;    

public:
    explicit JoinMR(char* Data_A, char* Data_B, char* Data_A_key, char* Data_B_key, uint64_t LenA, uint64_t LenB, int Chunk_size, int Key_Column) : data_A(Data_A), data_B(Data_B),data_A_key(Data_A_key), data_B_key(Data_B_key), lenA(LenA), lenB(LenB), splitter_pos_A(0),splitter_pos_B(0), chunk_size(Chunk_size), key_column(Key_Column) {}

    // void *locate (data_type *data, uint64_t len) const
    // {
    //     return data->keys;
    // }

    // void* locate(wc_string* d, uint64_t len) const
    // {
    //     return d->matrix_A + d->row_num * d->matrix_len;
    // }

    void map(data_type& s, map_container& out) const
    {   

        printf("started mapping A \n");

        uint64_t i_A = 0;
        uint64_t i_Value_A = 0;
        uint64_t start_key_A = 0;
        uint64_t start_Value_A = 0;
        //uint64_t key_start, key_end;
        int column_counter = 0;
        bool first_column = false;
        wc_word word;
        char *key = NULL, *value = NULL;


        while(i_Value_A < s.lenA)
        { 
            column_counter = 0;

            while(i_Value_A < s.lenA && (s.data_A[i_A] == '\r' || s.data_A[i_A] == '\n' || s.data_A[i_A] == '\t'))
            {
                i_Value_A++;
            }

            while(i_A < s.lenA && (s.data_A_key[i_A] != '|'))
            {
                if (key_column == 0) {

                    first_column = true;
                    start_key_A = i_A;
                    break;
                }
                else {

                    i_A++;
                }
            }

            column_counter++;
            i_A++;
            do {
                while(i_A < s.lenA && (s.data_A_key[i_A] != '|'))
                {
                    i_A++;
                    // printf("i_A in do while loop: %i \n", i_A);
                }
                column_counter++;
                i_A++;
               // printf("column counter in do while loop: %i \n", column_counter);
            }
            while (column_counter != key_column);
             
            if ((key_column == column_counter) && !first_column)
            {
                
                start_key_A = i_A ;
                //i_A++;

                while(i_A < s.lenA && (s.data_A[i_A] != '|')) {

                   // printf("i_A in loop: %i \n", i_A);
                    i_A++;
                    
                }
                column_counter++;
                //i_A++;
                //printf("column counter: %i \n", column_counter);

                if(i_A > start_key_A)
                {
                    //printf("in the loop\n");
                    //(void*)s.data_A_key[i_A] = 0;
                    

                    s.data_A_key[i_A] = 0;

                    printf("keyA: %c \n",  s.data_A_key[start_key_A]);
                    printf("keyA: %c \n",  s.data_A_key[i_A-1]);
                    printf("\n\n");

                    word = { s.data_A_key + start_key_A };

                    
                }
            }

            if (first_column == true)
            {
                i_A++;

                while(i_A < s.lenA && (s.data_A[i_A] != '|'))
                    i_A++;

                if(i_A > start_key_A)
                {

                    s.data_A_key[i_A] = 0;
                    word = { s.data_A_key + start_key_A };
                    printf("keyA: %c \n",  s.data_A_key[start_key_A]);
                    printf("keyA: %c \n",  s.data_A_key[i_A-1]);
                }
            }
                
            column_counter = 0;

            start_Value_A = i_Value_A;

            while(i_Value_A < s.lenA && (s.data_A[i_Value_A] != '\r' && s.data_A[i_Value_A] != '\n' && s.data_A[i_Value_A] != '\0')) 
            {
                //printf("data_A @ ivalA: %c \n", s.data_A[i_Value_A-1]);
                i_Value_A++;
            }

            i_A = i_Value_A;

            if(i_Value_A > start_Value_A)
                {

                    s.data_A[i_Value_A] = 0; // end of the value

                    int identifierA = 0;
                    valueStruct outputA(identifierA, s.data_A + start_Value_A);

                    printf("valA: %c \n",  s.data_A[start_Value_A]);
                    // printf("valA: %i \n",  (int)s.data_A[i_Value_A]);
                    printf("valA2: %c \n",  s.data_A[i_Value_A]);

                    // printf("value mapped data: ");
                    // for (uint64_t l = start_Value_A; l < i_Value_A; l++)
                    // {
                        
                    //     printf("%c", s.data_A[l]);
                        
                    // }
                    // printf("\n\n");

                    emit_intermediate(out, word, outputA);

                    key = NULL;
                    value = NULL;

                }
            //printf("Ival: %u \n", i_Value_A);
            i_Value_A++;
            i_A++;
        }

        printf("started mapping B \n");


        uint64_t i_B = 0;
        uint64_t i_Value_B = 0;
        uint64_t start_key_B = 0;
        uint64_t start_Value_B = 0;
        first_column = false;
        column_counter = 0;
        key = NULL;
        value = NULL;

        while(i_Value_B < s.lenB)
        { 
            column_counter = 0;

            while(i_Value_B < s.lenB && (s.data_B[i_B] == '\r' || s.data_B[i_B] == '\n' || s.data_B[i_B] == '\t'))
            {
                i_Value_B++;
            }

            while(i_B < s.lenB && (s.data_B_key[i_B] != '|'))
            {
                if (key_column == 0) 
                {
                    first_column = true;
                    start_key_B = i_B;
                    break;
                }
                else
                {
                    i_B++;
                }
            }

            column_counter++;
            i_B++;

            do {
                while(i_B < s.lenB && (s.data_B_key[i_B] != '|'))
                {
                    i_B++;
                }
                column_counter++;
                i_B++;
            }
            while (column_counter != key_column);

            
            if ((key_column == column_counter) && !first_column)
            {
                start_key_B = i_B ;
                //i_B++;

                while(i_B < s.lenB && (s.data_B[i_B] != '|'))
                {
                    i_B++;
                }

                column_counter++;
               // i_B++;

                if(i_B > start_key_B)
                {
                    s.data_B_key[i_B] = 0;
                    word = { s.data_B_key + start_key_B };

                    printf("keyB: %c \n",  s.data_B_key[start_key_B]);
                    printf("keyB: %c \n",  s.data_B_key[i_B-1]);

                }
            }
            if (first_column)
            {
                i_B++;

                while(i_B < s.lenB && (s.data_B[i_B] != '|'))
                {
                    i_B++;
                }
                column_counter++;

                if(i_B > start_key_B)
                {

                    s.data_B_key[i_B] = 0;
                    word = { s.data_B_key + start_key_B };
                    printf("keyB: %c \n",  s.data_B_key[start_key_B]);
                    printf("keyB: %c \n",  s.data_B_key[i_B-1]);

                }
            }

            column_counter = 0;
            

            start_Value_B = i_Value_B;

            while(i_Value_B < s.lenB && (s.data_B[i_Value_B] != '\r' && s.data_B[i_Value_B] != '\n' && s.data_B[i_Value_B] != '\0'))
            {
                i_Value_B++;
            }

            i_B = i_Value_B;

            // while(s.data_B[i_B+1] == '\r' || s.data_B[i_B+1] == '\n')
            // {
            //     printf("ANOTHER END OF LINE FOUND");
            // }

            if(i_Value_B > start_Value_B)
            {
                
                s.data_B[i_Value_B] = 0; // end of the value
                int identifierB = 1;




                valueStruct outputB( identifierB, s.data_B + start_Value_B );

                printf("valB: %c \n",  s.data_B[start_Value_B]);
                printf("valB: %c \n",  s.data_B[i_Value_B]);

                //printf("value mapped data: ");
                // for (uint64_t l = start_Value_B; l < i_Value_B; l++)
                // {
                    
                //     printf("%c", s.data_B[l]);
                    
                // }
                // printf("\n\n");


                emit_intermediate(out, word, outputB);

                key = NULL;
                value = NULL;
            }
            i_B++;
            i_Value_B++;
        }
    }

    /** string_match_split()
     *  Splitter Function to assign portions of the file to each map task
     */
    int split(wc_string& out)
    {
        /* End of data reached, return FALSE. */
        if ((uint64_t)splitter_pos_A >= lenA)
        {
            return 0;
        }

        if ((uint64_t)splitter_pos_B >= lenB)
        {
            return 0;
        }

        /* Determine the nominal end point. */
        uint64_t endA = std::min(splitter_pos_A + chunk_size, lenA);
        uint64_t endB = std::min(splitter_pos_B + chunk_size, lenB);

        //AAAAAAAAAAAAAAA

        /* Move end point to next word break */
        while(endA < lenA && data_A[endA] != '\r' && data_A[endA] != '\n')
            endA++;

        /* Set the start of the next data. */
        out.data_A = data_A + splitter_pos_A;
        out.data_A_key = data_A_key + splitter_pos_A;

        out.lenA = endA - splitter_pos_A;
        
        // Skip line breaks...
        while(endA < lenA && data_A[endA] == '\r' && data_A[endA] == '\n')
            endA++;

        splitter_pos_A = endA;

        //BBBBBBBBBBBBBBBB

        /* Move end point to next word break */
        while(endB < lenB && data_B[endB] != '\r' && data_B[endB] != '\n')
            endB++;

        /* Set the start of the next data. */
        out.data_B = data_B + splitter_pos_B;
        out.data_B_key = data_B_key + splitter_pos_B;

        out.lenB = endB - splitter_pos_B;
        
        // Skip line breaks...
        while(endB < lenB && data_B[endB] == '\r' && data_B[endB] == '\n')
            endB++;
            
        splitter_pos_B = endB;

        printf("finished splitting \n");

        /* Return true since the out data is valid. */
        return 1;
    }

    // void reduce(key_type const& key, reduce_iterator const& values, std::vector<keyval>& out) const 
    // {

    //     value_type val;
    //     while (values.next(val))
    //     {
    //         keyval kv = {key, val};
    //         out.push_back(kv);
    //     }
    // }

    void reduce(key_type const& key, reduce_iterator const& values, std::vector<keyval>& out) const 
    {
        printf("entering reducer \n");
        value_type val;
        std::vector<value_type> array1;
        std::vector<value_type> array2;

        //printf("in Reduce");

        while (values.next(val))
        {
            if (val.identifier == 0)
            {
                //printf("brfore push");
                array1.push_back(val);
                // printf("identifierA: %i\n", array1[0].identifier);
                // printf("value_data_A: %c\n", array1[0].value_data[0]);
            }
            if (val.identifier == 1)
            {
                array2.push_back(val);
                // printf("identifierB: %i\n", array2[0].identifier);
                // printf("value_data_B: %c\n", array2[0].value_data[0]);
            }
        }

        // if (array1.size() == 0 && array2.size() == 0)
        // {
        //     key_type newKey;
        //     char* space;
        //     space = (char *)malloc(2);
        //     space[0] = ' ';
        //     space[1] = 0;
        //     newKey.data = space;

        //     //value_type newValue;
        //     char* value;
        //     value = (char *)malloc(11);
        //     space[0] = 'n';
        //     space[1] = 'n';
        //     space[2] = 'n';
        //     space[3] = 'n';
        //     space[4] = 'n';
        //     space[5] = 'n';
        //     space[6] = 'n';
        //     space[7] = 'n';
        //     space[8] = 'n';
        //     space[9] = 'n';
        //     space[10] = 0;
        //     //newValue.value_data = value;

        //     value_type myvalue(-1, value);
        //     keyval kv = {key, myvalue};
        //     out.push_back(kv);

        //     return;
        // }

        for (int i=0; i < array1.size(); i++) 
        {
            for (int j=0; j < array2.size(); j++) 
            {
                // printf("array1 data: ");
                // for (uint64_t l = 0; l < strlen(array1[i].value_data); l++)
                // {
                    
                //     printf("%c", array1[i].value_data[l]);
                    
                // }

                uint64_t length = strlen(array1[i].value_data) + strlen(array2[j].value_data);

                //printf("%u %u %u \n", strlen(array1[i].value_data), strlen(array2[j].value_data) , length );

                char* result;
                result = (char *)malloc(length+2);

                strcpy(result, array2[j].value_data);
                strcat(result, array1[i].value_data);

                result[length+1] = 0;

                value_type myvalue(-1, result);
                keyval kv = {key, myvalue};
                out.push_back(kv);

                // printf("key data: %c ", key.data[0]);
                // printf("key data: %c ", key.data[1]);
                // printf("key data: %c \n", key.data[2]);

                // printf("value data: %c ", myvalue.value_data[0]);
                // printf("value data: %c ", myvalue.value_data[1]);
                // printf("value data: %c ", myvalue.value_data[2]);
                // printf("value data: %c ", myvalue.value_data[3]);
                // printf("value data: %c \n\n", myvalue.value_data[4]);
            }
        }
    }



 };

//#define NO_MMAP
//#define MMAP_POPULATE

int main(int argc, char *argv[]) {

    // Remove output file if it exists
    if (remove("mapReduceOutput.txt") != 0) {
        printf("Error removing existing output file\n\n");
    } else {
        printf("Removed existing output file");
    }
    
    int fd_A, fd_B, fd_Aa, fd_Bb;                           // file open flag
    char *fname_A, *fname_B, *fname_Aa, *fname_Bb;          // filename
    char *fdata_A, *fdata_B, *fdata_A_key, *fdata_B_key ;        // file data
    struct stat finfo_A, finfo_B, finfo_Aa, finfo_Bb;   // file info (file length)
    int key_column;

    //struct timespec begin, end;

    //get_time (begin);

    if (argv[1] == NULL)
    {
        printf("USAGE: %s <filename1 filename2 key_column>\n", argv[0]);
        exit(1);
    }

    fname_A = argv[1];
    fname_B = argv[2];
    fname_Aa = argv[4];
    fname_Bb = argv[5];
    key_column = atoi(argv[3]);

    printf("Eqiu Join: Running...\n");

    // AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA

    // Read in the file
    CHECK_ERROR((fd_A = open(fname_A,O_RDWR)) < 0);

    // Get the file info (for file length)
    CHECK_ERROR(fstat(fd_A, &finfo_A) < 0);

#ifndef NO_MMAP
#ifdef MMAP_POPULATE
    // Memory map the file
    CHECK_ERROR((fdata_A = (char*)mmap(NULL, finfo_A.st_size + 1, 
        PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd_A, 0)) == NULL);
#else
    // Memory map the file
    CHECK_ERROR((fdata_A = (char*)mmap(NULL, finfo_A.st_size + 1, 
        PROT_READ | PROT_WRITE, MAP_SHARED , fd_A, 0)) == NULL);

    close(fd_A);

    CHECK_ERROR((fd_Aa = open(fname_Aa,O_RDWR)) < 0);
    CHECK_ERROR(fstat(fd_Aa, &finfo_Aa) < 0);

    CHECK_ERROR((fdata_A_key = (char*)mmap(NULL, finfo_Aa.st_size + 1, 
        PROT_READ | PROT_WRITE  , MAP_SHARED, fd_Aa, 0)) == NULL);
#endif
#else

    fdata_A = (char *)malloc (finfo_A.st_size);
    CHECK_ERROR (fdata_A == NULL);

    fdata_A_key = (char *)malloc (finfo_A.st_size);
    CHECK_ERROR (fdata_A_key == NULL);


    uint64_t r = 0;

    while (r < (uint64_t)finfo_A.st_size ) {
        r += pread (fd_A, fdata_A + r , finfo_A.st_size, r);
    }
    fdata_A[finfo_A.st_size] = 0;
    CHECK_ERROR(r != (uint64_t)finfo_A.st_size);

    close(fd_A);


    CHECK_ERROR((fd_A = open(fname_A,O_RDONLY)) < 0);

    r = 0;

    while (r < (uint64_t)finfo_A.st_size ) {
        r += pread (fd_A, fdata_A_key + r , finfo_A.st_size, r);
    }
    fdata_A_key[finfo_A.st_size] = 0;
    CHECK_ERROR(r != (uint64_t)finfo_A.st_size);
    
#endif

    // BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB

    // Read in the file
    CHECK_ERROR((fd_B = open(fname_B,O_RDWR)) < 0);
    // Get the file info (for file length)
    CHECK_ERROR(fstat(fd_B, &finfo_B) < 0);

#ifndef NO_MMAP
#ifdef MMAP_POPULATE
    // Memory map the file
    CHECK_ERROR((fdata_B = (char*)mmap(NULL, finfo_B.st_size + 1, 
        PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd_B, 0)) == NULL);
#else
    // Memory map the file
    //void* vp = (void*)fdata_A;
    CHECK_ERROR((fdata_B = (char*)mmap(NULL, finfo_B.st_size + 1, 
    //CHECK_ERROR((fdata_B = (char*)mmap(NULL, finfo_B.st_size + 1, 
        PROT_READ | PROT_WRITE, MAP_PRIVATE , fd_B, 0)) == NULL);

    close(fd_B);

    CHECK_ERROR((fd_Bb = open(fname_Bb,O_RDWR)) < 0);
    CHECK_ERROR(fstat(fd_Bb, &finfo_Bb) < 0);

    CHECK_ERROR((fdata_B_key = (char*)mmap(NULL, finfo_Bb.st_size + 1, 
        PROT_READ | PROT_WRITE, MAP_PRIVATE, fd_Bb, 0)) == NULL);
#endif
#else

    fdata_B = (char *)malloc (finfo_B.st_size);
    CHECK_ERROR (fdata_B == NULL);

    fdata_B_key = (char *)malloc (finfo_B.st_size);
    CHECK_ERROR (fdata_B_key == NULL);


    r = 0;

    while (r < (uint64_t)finfo_B.st_size ) {
        r += pread (fd_B, fdata_B + r , finfo_B.st_size, r);
    }
    fdata_B[finfo_B.st_size] = 0;
    CHECK_ERROR(r != (uint64_t)finfo_B.st_size);

    close(fd_B);


    CHECK_ERROR((fd_B = open(fname_B,O_RDONLY)) < 0);

    r = 0;

    while (r < (uint64_t)finfo_B.st_size ) {
        r += pread (fd_B, fdata_B_key + r , finfo_B.st_size, r);
    }
    fdata_B_key[finfo_B.st_size] = 0;
    CHECK_ERROR(r != (uint64_t)finfo_B.st_size);

#endif

    // for (int i = 0; i < 50; i++)
    // {
    //     printf("%c", fdata_B[i]);
    //     // cout << fdata_B[i] << endl;
    // }

    // cout << endl;

    // for (int i = 0; i < 50; i++)
    // {
    //     printf("%c", fdata_B_key[i]);
    //     // cout << fdata_B[i] << endl;
    // }

    cout << endl;

    printf("Equi Join: Calling Equi Join\n");

    // for (int i = 0; i < 50; i++)
    // {
    //     printf("%s \n", fdata_A[i]);
    // }

    struct timeval tv1, tv2;
    gettimeofday(&tv1, NULL);
    JoinMR mapReduce(fdata_A, fdata_B, fdata_A_key, fdata_B_key, finfo_A.st_size, finfo_B.st_size, 64*1024, key_column);
    std::vector<JoinMR::keyval> out;

    CHECK_ERROR (mapReduce.run(out) < 0);

    if(out.size() != 0) {

        ofstream outputFile;
        outputFile.open("mapReduceOutput.txt", std::ios::app);

        for (size_t i = 0; i < out.size(); i++)
        {
            //printf("%15s - %100s\n", out[out.size()-1-i].key.data, out[out.size()-1-i].val.value_data);
            outputFile << out[out.size()-1-i].key.data << ": \t" << out[out.size()-1-i].val.value_data<< "\n";
        }
        outputFile.close();
    }

    printf("%i \n", out.size());
    printf("Equi Join: MapReduce Completed\n");
    gettimeofday(&tv2, NULL);

    printf("%f seconds\n", (double) (tv2.tv_usec - tv1.tv_usec) / CLOCKS_PER_SEC + (double) (tv2.tv_sec - tv1.tv_sec));

    // printf("%i:  arg 3\n", key_column);
    

    //print_time("library", begin, end);

    //get_time (begin);

// free the mapped memory
#ifndef NO_MMAP
    CHECK_ERROR(munmap(fdata_A, finfo_A.st_size + 1) < 0);
    CHECK_ERROR(munmap(fdata_B, finfo_B.st_size + 1) < 0);
    CHECK_ERROR(munmap(fdata_A_key, finfo_A.st_size + 1) < 0);
    CHECK_ERROR(munmap(fdata_B_key, finfo_B.st_size + 1) < 0);
#else
    free (fdata_A);
    free (fdata_A_key);
    free (fdata_B);
    free (fdata_B_key);
#endif
    // close the file
    CHECK_ERROR(close(fd_A) < 0);
    CHECK_ERROR(close(fd_B) < 0);

    //get_time (end);

    //print_time("finalize", begin, end);

    return 0;
}

// vim: ts=8 sw=4 sts=4 smarttab smartindent
