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
using namespace std;

#include "map_reduce.h"

#define DEFAULT_UNIT_SIZE 5
#define SALT_SIZE 2
#define MAX_REC_LEN 1024
#define OFFSET 5
#define NO_MMAP

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

    char* value_data;
    int identifier;

    valueStruct() { value_data = NULL; identifier = -1;}
    valueStruct(char* value_data, int identifier) { this->value_data = value_data; this->identifier = identifier; }
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
    explicit JoinMR(char* Data_A, char* Data_B, char* Data_A_key, char* Data_B_key, uint64_t LenA, uint64_t LenB, int Chunk_size) : data_A(Data_A), data_B(Data_B),data_A_key(Data_A_key), data_B_key(Data_B_key), lenA(LenA), lenB(LenB), splitter_pos_A(0),splitter_pos_B(0), chunk_size(Chunk_size), key_column(1) {}

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
        int column_counter = 0;
        bool first_column = false;
        wc_word word;


        while(i_Value_A < s.lenA)
        { 
            column_counter = 0;

            while(i_Value_A < s.lenA && (s.data_A[i_A] == '\r' || s.data_A[i_A] == '\n' || s.data_A[i_A] == '\t'))
            {
                //printf("start main while loop \n");
                //printf("counter: %u \n", i_A);
                //printf("len: %u \n", lenA);
                //printf("val: %c \n", s.data_A_key[i_A]);

                i_Value_A++;
            }
            printf("%u\n\n", i_Value_A);

            while(i_A < s.lenA && (s.data_A_key[i_A] != '|'))
            {
                //printf("i_A: %i \n", i_A);
                //printf("val: %c \n", s.data_A[i_A]);
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
            

            if ((key_column == column_counter))
            {
                
                start_key_A = i_A+1;
                i_A++;

                while(i_A < s.lenA && (s.data_A[i_A] != '|')) {

                    //printf("i_A in loop: %i \n", i_A);
                    i_A++;
                    
                }
                column_counter++;
                printf("column counter: %i \n", column_counter);

                if(i_A > start_key_A)
                {
                    //printf("in the loop\n");
                    s.data_A_key[i_A] = 0;

                    printf("key: %c \n",  s.data_A_key[start_key_A]);
                    printf("key: %c \n",  s.data_A_key[i_A-1]);

                    word = { s.data_A_key + start_key_A };
                }
            }

            if (first_column == true)
            {
                while(i_A < s.lenA && (s.data_A[i_A] != '|'))
                    i_A++;

                if(i_A > start_key_A)
                {
                    s.data_A_key[i_A] = 0;
                    word = { s.data_A_key + start_key_A };
                }
            }
                
            column_counter = 0;

            start_Value_A = i_Value_A;

            while(i_Value_A < s.lenA && (s.data_A[i_Value_A] != '\r' && s.data_A[i_Value_A] != '\n' && s.data_A[i_Value_A] != '\0')) 
            {
                printf("data_A @ ivalA: %c \n", s.data_A[i_Value_A-1]);
                i_Value_A++;
            }

            i_A = i_Value_A;

            if(i_Value_A > start_Value_A)
                {
                    s.data_A[i_Value_A] = 0; // end of the value
                    int identifierA = 0;
                    valueStruct outputA(s.data_A + start_Value_A, identifierA);

                    printf("val: %c \n",  s.data_A + start_Value_A);
                    printf("val: %c \n",  s.data_A[i_Value_A]);

                    emit_intermediate(out, word, outputA);
                }
            printf("Ival: %u \n", i_Value_A);
            i_Value_A++;
        }

        printf("started mapping B \n");


        uint64_t i_B = 0;
        uint64_t i_Value_B = 0;
        uint64_t start_key_B = 0;
        uint64_t start_Value_B = 0;
        first_column = false;
        column_counter = 0;

        while(i_Value_B < s.lenB)
        { 
            while(i_Value_B < s.lenB && (s.data_B[i_Value_B] != '\r' || s.data_B[i_Value_B] != '\n'))
            {
                i_Value_B++;
                while(i_B < s.lenB && (s.data_B_key[i_B] != '|'))
                {
                    if (key_column == 0) {
                    first_column = true;
                    start_key_B = i_B;
                    }
                    else
                        i_B++;
                }
                column_counter++;

                if (key_column == column_counter && !first_column)
                {
                    start_key_B = i_B+1;
                    while(i_B < s.lenB && (s.data_B[i_B] != '|'))
                        i_B++;

                    if(i_B > start_key_B)
                    {
                        s.data_B_key[i_B] = 0;
                        word = { s.data_B_key + start_key_B };
                    }
                }
                if (first_column)
                {
                    while(i_B < s.lenB && (s.data_B[i_B] != '|'))
                        i_B++;

                    if(i_B > start_key_B)
                    {
                        s.data_B_key[i_B] = 0;
                        word = { s.data_B_key + start_key_B };
                    }
                }
            }

            start_Value_B = i_Value_B;
            while(i_Value_B < s.lenB && (s.data_B[i_Value_B] != '\r' || s.data_B[i_Value_B] != '\n'))
                    i_Value_B++;
            if(i_Value_B > start_Value_B)
                {
                    s.data_B[i_Value_B] = 0; // end of the value
                    int identifierB = 0;
                    valueStruct outputB(s.data_B + start_Value_B, identifierB);
                    printf("%c \n", word.data[0]);
                    printf("%c \n", word.data[1]);
                    printf("OUTPUT:\n");
                    printf("%c \n", outputB.value_data[0]);
                    emit_intermediate(out, word, outputB);
                }
            
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

    void reduce(key_type const& key, reduce_iterator const& values, std::vector<keyval>& out) const {
        value_type val;
        std::vector<value_type> array1;
        std::vector<value_type> array2;
        while (values.next(val))
        {
            if (val.identifier == 0)
            {
                array1.push_back(val);
            }
            else if (val.identifier == 1)
                array2.push_back(val);
        }

        for (int i=0; i < array1.size(); i++) {
            for (int j=0; j < array2.size(); j++) {
                char* joinedValue = strcpy(array2[j].value_data, array1[j].value_data);
                value_type myvalue(joinedValue, -1);
                keyval kv = {key, myvalue};
                out.push_back(kv);
            }
        }
    }
};




int main(int argc, char *argv[]) {
    
    int fd_A, fd_B;                 // file open flag
    char *fname_A, *fname_B;        // filename

    char *fdata_A, *fdata_B, *fdata_A_key, *fdata_B_key ;        // file data

    struct stat finfo_A, finfo_B;   // file info (file length)
    //char *fname_keys;

    struct timespec begin, end;

    get_time (begin);

    if (argv[1] == NULL)
    {
        printf("USAGE: %s <keys filename>\n", argv[0]);
        exit(1);
    }

    fname_A = argv[1];
    fname_B = argv[2];

    printf("Eqiu Join: Running...\n");

    // AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA

    // Read in the file
    CHECK_ERROR((fd_A = open(fname_A,O_RDONLY)) < 0);

    // Get the file info (for file length)
    CHECK_ERROR(fstat(fd_A, &finfo_A) < 0);

#ifndef NO_MMAP
#ifdef MMAP_POPULATE
    // Memory map the file
    //CHECK_ERROR((fdata_A = (int*)mmap(0, finfo_A.st_size + 1, 
    //    PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd_A, 0)) == NULL);
#else
    // Memory map the file
    CHECK_ERROR((fdata_A = (char*)mmap(0, finfo_A.st_size + 1, 
        PROT_READ, MAP_PRIVATE, fd_A, 0)) == NULL);

    CHECK_ERROR((fdata_A_key = (char*)mmap(0, finfo_A.st_size + 1, 
        PROT_READ, MAP_PRIVATE, fd_A, 0)) == NULL);
#endif
#else
    int ret, rety;

    fdata_A = (char *)malloc (finfo_A.st_size);
    CHECK_ERROR (fdata_A == NULL);

    fdata_A_key = (char *)malloc (finfo_A.st_size);
    CHECK_ERROR (fdata_A_key == NULL);

    uint64_t r = 0;

    while (r < (uint64_t)finfo_A.st_size ) {
        r += pread (fd_A, fdata_A + r , finfo_A.st_size, r);
    }
    CHECK_ERROR(r != (uint64_t)finfo_A.st_size);

    // ret = read (fd_A, fdata_A, finfo_A.st_size);
    // CHECK_ERROR (ret != finfo_A.st_size);
    // printf("before \n");
    // printf("%s", fdata_A[0]);
    printf("after \n");


    close(fd_A);

    // for (size_t i = 0; i < 20; i++)
    // {
    //     printf("%50s\n", fdata_A[finfo_A.st_size-1-i]);
    // }

    CHECK_ERROR((fd_A = open(fname_A,O_RDONLY)) < 0);

    r = 0;

    while (r < (uint64_t)finfo_A.st_size ) {
        r += pread (fd_A, fdata_A_key + r , finfo_A.st_size, r);
    }
    CHECK_ERROR(r != (uint64_t)finfo_A.st_size);
    // rety = read (fd_A, fdata_A_key, finfo_A.st_size);
    // CHECK_ERROR (rety != finfo_A.st_size);
    
#endif

    // BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB

    // Read in the file
    CHECK_ERROR((fd_B = open(fname_B,O_RDONLY)) < 0);
    // Get the file info (for file length)
    CHECK_ERROR(fstat(fd_B, &finfo_B) < 0);

#ifndef NO_MMAP
#ifdef MMAP_POPULATE
    // Memory map the file
    // CHECK_ERROR((fdata_B = (int*)mmap(0, finfo_B.st_size + 1, 
    //     PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd_B, 0)) == NULL);
#else
    // Memory map the file
    CHECK_ERROR((fdata_B = (char*)mmap(0, finfo_B.st_size + 1, 
        PROT_READ, MAP_PRIVATE, fd_B, 0)) == NULL);

    CHECK_ERROR((fdata_B_key = (char*)mmap(0, finfo_B.st_size + 1, 
        PROT_READ, MAP_PRIVATE, fd_B, 0)) == NULL);
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
    CHECK_ERROR(r != (uint64_t)finfo_B.st_size);

    // ret = read (fd_B, fdata_B, finfo_B.st_size);
    // CHECK_ERROR (ret != finfo_B.st_size);

    close(fd_B);
    CHECK_ERROR((fd_B = open(fname_B,O_RDONLY)) < 0);

    r = 0;

    while (r < (uint64_t)finfo_B.st_size ) {
        r += pread (fd_B, fdata_B_key + r , finfo_B.st_size, r);
    }
    CHECK_ERROR(r != (uint64_t)finfo_B.st_size);

    // ret = read (fd_B, fdata_B_key, finfo_B.st_size);
    // CHECK_ERROR (ret != finfo_B.st_size);

    // fdata_B = (char *)malloc (finfo_B.st_size);
    // CHECK_ERROR (fdata_B == NULL);

    // ret = read (fd_B, fdata_B, finfo_B.st_size);
    // CHECK_ERROR (ret != finfo_B.st_size);

    // fdata_A = (char*)malloc(finfo_A.st_size);
    // fdata_B = (char*)malloc(finfo_B.st_size);

    // CHECK_ERROR (fdata_A == NULL);
    // CHECK_ERROR (fdata_B == NULL);
#endif
    
    get_time (end);

    print_time("initialize", begin, end);

    printf("Equi Join: Calling Equi Join\n");

    // for (int i = 0; i < 50; i++)
    // {
    //     printf("%s \n", fdata_A[i]);
    // }

    get_time (begin);

    JoinMR mapReduce(fdata_A, fdata_B, fdata_A_key, fdata_B_key, finfo_A.st_size, finfo_B.st_size, 64*1024);
    std::vector<JoinMR::keyval> out;

    CHECK_ERROR (mapReduce.run(out) < 0);
    get_time (end);

    // for (size_t i = 0; i < 20; i++)
    // {
    //     printf("%15s - %50s\n", out[out.size()-1-i].key.data, out[out.size()-1-i].val.value_data);
    // }

    printf("%i \n", out.size());
    printf("Equi Join: MapReduce Completed\n");

    print_time("library", begin, end);

    get_time (begin);

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

    get_time (end);

    print_time("finalize", begin, end);

    return 0;
}

// vim: ts=8 sw=4 sts=4 smarttab smartindent
