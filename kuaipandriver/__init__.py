import os
from sys import argv
from kuaipanfuse import KuaiPanFuse
from fuse import FUSE as FusePy

__all__ = ['FusePy', 'KuaiPanFuse']


def main():

    if __name__ == "__main__":

       if len(argv) != 2:
            print('usage: %s <mountpoint>' % argv[0])
            exit(1)

       gid, uid = os.getgid(), os.getuid()
       FusePy(
               KuaiPanFuse(), 
               argv[1], 
               foreground=True, 
               nothreads=False, 
               debug=True, 
               direct_io=False, 
               gid=os.getgid(), 
               uid=os.getuid(),
               allow_other=True, 
               umask='0133'
               )
