#include <iostream>
#include <sys/types.h>    
#include <sys/stat.h>    
#include <fcntl.h>
#include <string>
#include <stdio.h>
#include <unistd.h>
#include <string.h>

int main()
{
    int fd, size;
    std::string s = "Linux Programmer!"; 
    char buffer[80];
    
    fd = open("/tmp/temp", O_CREAT | O_RDWR);
    write(fd, s.c_str(), s.length());
    //close(fd_write);
    //fd_read = open("/tmp/temp", O_RDONLY);
    //size = pread(fd_read, buffer, 4, 0);
    //printf("%s", buffer);

    //std::cout << sizeof(s) << " " << strlen(s) << std::endl;
    write(fd, s.c_str(), s.length());
    size = pread(fd, buffer, 4, 0);
    printf("%s", buffer);
    write(fd, s.c_str(), s.length());
    write(fd, s.c_str(), s.length());
    //size = pread(fd_read, buffer, 4, 0);
    //printf("%s", buffer);
    close(fd);
    return 0;
}

