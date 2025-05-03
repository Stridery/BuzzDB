#include "external_sort/external_sort.h"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <functional>
#include <iostream>
#include <map>
#include <queue>
#include <thread>
#include <vector>

#include "storage/test_file.h"

using namespace std;

#define UNUSED(p) ((void)(p))

namespace buzzdb {

void merge(std::vector<std::unique_ptr<File>>& chunk_files, size_t mem_size){
    int length = chunk_files.size();
    int lower = 0, higher = min(9, length);
    int merge_max_ways = higher - lower;
    size_t chunk_size;


    vector<unique_ptr<File>> output_chunk_files;
    while(lower < length){
        int ways = min(merge_max_ways, length - lower);
        chunk_size = mem_size / ((ways + 2) * sizeof(uint64_t));
        if(ways > 1){
            vector<unique_ptr<uint64_t[]>> chunk_pointer(ways); // 每个file当前操作的chunk
            vector<size_t> num_pointer(ways, 0); // 每个chunk当前操作的数在chunk中的idx
            vector<size_t> chunk_size_each(ways); // 每个chunk的size
            vector<size_t> chunk_offsets(ways, 0); // 每个chunk在file中的起始offset

            priority_queue<pair<uint64_t, int>, vector<std::pair<uint64_t, int>>, greater<pair<uint64_t, int>>> pq; // 数和来自的chunk

            unique_ptr<File> sorted_temp_file;
            sorted_temp_file = move(File::make_temporary_file());
            auto chunk = make_unique<uint64_t[]>(chunk_size);
            size_t idx = 0;
            size_t write_offset = 0;
            for(int i = 0; i < ways; i++){
                chunk_size_each[i] = min(chunk_files[i + lower]->size() / sizeof(uint64_t), chunk_size);
                unique_ptr<uint64_t[]> this_chunk = make_unique<uint64_t[]>(chunk_size_each[i]);
                chunk_pointer[i] = move(this_chunk);
                size_t read_size = min(chunk_files[i + lower]->size() / sizeof(uint64_t) - chunk_offsets[i], chunk_size);
                chunk_files[i + lower]->read_block(chunk_offsets[i] * sizeof(uint64_t), read_size * sizeof(uint64_t), reinterpret_cast<char*>(chunk_pointer[i].get()));
                chunk_offsets[i] += read_size;
                pq.push(make_pair(chunk_pointer[i][0], i));
            }
            while(!pq.empty()){
                uint64_t num = pq.top().first;
                size_t chunk_idx = pq.top().second;
                pq.pop();
                chunk[idx++] = num;
                if(idx == chunk_size){
                    sorted_temp_file->resize(sorted_temp_file->size() + idx * sizeof(uint64_t));
                    sorted_temp_file->write_block(reinterpret_cast<char *>(chunk.get()), write_offset * sizeof(uint64_t), idx * sizeof(uint64_t));
                    write_offset += idx;
                    idx = 0;
                }
                num_pointer[chunk_idx]++;

                if(num_pointer[chunk_idx] == chunk_size_each[chunk_idx]){
                    if(chunk_offsets[chunk_idx] < chunk_files[chunk_idx + lower]->size() / sizeof(uint64_t)){
                        size_t read_size = min(chunk_files[chunk_idx + lower]->size() /sizeof(uint64_t) - chunk_offsets[chunk_idx], chunk_size);
                        chunk_files[chunk_idx + lower]->read_block(chunk_offsets[chunk_idx] * sizeof(uint64_t), read_size * sizeof(uint64_t), reinterpret_cast<char*>(chunk_pointer[chunk_idx].get()));
                        num_pointer[chunk_idx] = 0;
                        chunk_size_each[chunk_idx] = read_size;
                        chunk_offsets[chunk_idx] += read_size;

                        pq.push(make_pair(chunk_pointer[chunk_idx][num_pointer[chunk_idx]], chunk_idx));
                    }
                }
                else{
                    pq.push(make_pair(chunk_pointer[chunk_idx][num_pointer[chunk_idx]], chunk_idx));
                }
            }
            
            if(idx){
                sorted_temp_file->resize(sorted_temp_file->size() + idx * sizeof(uint64_t));
                sorted_temp_file->write_block(reinterpret_cast<char *>(chunk.get()), write_offset * sizeof(uint64_t), idx * sizeof(uint64_t));  
            }
            
            
            output_chunk_files.push_back(move(sorted_temp_file));
            
        }
        else{
            output_chunk_files.push_back(move(chunk_files[lower]));
        }
        lower += ways;
    }
    chunk_files.clear();
    chunk_files.assign(make_move_iterator(output_chunk_files.begin()), make_move_iterator(output_chunk_files.end()));
        
}



void external_sort(File &input, size_t num_values, File &output, size_t mem_size) {
    /* To be implemented
    ** Remove these before you start your implementation
    */
    size_t chunk_size = (mem_size / sizeof(uint64_t)) / 11;
    vector<unique_ptr<File>> chunk_files;
    
    uint64_t num_chunks = (num_values + chunk_size - 1) / chunk_size;
    
    for (uint64_t i = 0; i < num_chunks; ++i) {
        size_t offset = i * chunk_size;
        size_t read_size = chunk_size;
        size_t remaining_size = num_values - offset;
        
        if (remaining_size < read_size) {
            read_size = remaining_size;
        }
        auto chunk = make_unique<uint64_t[]>(read_size);
        input.read_block(offset * sizeof(uint64_t), read_size * sizeof(uint64_t), reinterpret_cast<char*>(chunk.get()));
        sort(chunk.get(), chunk.get() + read_size);
        unique_ptr<File> chunk_file = move(File::make_temporary_file());
        chunk_files.push_back(move(chunk_file));
        chunk_files.back()->resize(read_size * sizeof(uint64_t));
        chunk_files.back()->write_block(reinterpret_cast<char *>(chunk.get()), 0, read_size * sizeof(uint64_t));
    }
    while(chunk_files.size() > 1){
        merge(chunk_files, mem_size);
    } 
    size_t write_offset = 0;
    size_t last_chunk_size = mem_size / sizeof(uint64_t);


    while (write_offset < num_values) {
        size_t chunkSize = min(last_chunk_size, num_values - write_offset);
        auto block = make_unique<uint64_t[]>(chunk_files[0]->size() / sizeof(uint64_t));
        chunk_files[0]->read_block(write_offset * sizeof(uint64_t), chunkSize * sizeof(uint64_t), reinterpret_cast<char*>(block.get()));
        output.resize(output.size() + chunkSize * sizeof(uint64_t));
        output.write_block(reinterpret_cast<char *>(block.get()), write_offset * sizeof(uint64_t), chunkSize * sizeof(uint64_t));

        write_offset += chunkSize;
    }


    
    
}

}  // namespace buzzdb
