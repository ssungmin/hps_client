from datetime import date, timedelta
import os
import math
import time
import shutil
from pathlib import Path
import sys
from datetime import datetime
import time

#1. 할일 D-1 일 날짜로 data를 /DATA폴더로 전달 : 오후 2시?
#2. HOME 폴더에서 해당일 데이터 삭제
#3. /DATA 폴더에는 1주일 치의 데이터만 남기고 삭제 

# 폴더 없을시 폴더 생성


def createDirectory(directory: str, dt):
    try:
        
        
        name= (os.path.join(directory, dt))

        if not os.path.exists(name):
            os.makedirs(name)

            print(
                f"createDirectory: '{name}' Success To Make Directory File!! [Location -> {name}]"
            )
        return name

    # 권한이 없을 경우
    except PermissionError as e:
        content = 'sudo 또는 관리자 권한으로 실행 해주십시오...\n\n에러 내용 : {}'.format(e)
       #print(dividingAfter(' 권한이 없습니다. ', content))

        return False

    # 형식에 안맞을 경우
    except SyntaxError as e:
        print("{} 형식이 잘못되었습니다.".format(directory))
        print('SyntaxError : {}'.format(e))

    # 폴더 있을 경우
    except OSError:
        print(
            "createDirectory: '{}' Failed To Create The Directory...".format(
                name
            )
        )
        return name
    
def folder_file_copy(copyPath: str, pastePath: str, dt: str):
    
    
    try :
       os.chdir(copyPath)
       os.system("find . -name '*%s*' -exec cp -f {} --parents /DATA/pms_app/PMS_DATA/%s \;" % (dt,dt))

    except OSError:
        print("copy error")
    """
    file_dir = os.path.dirname(os.path.join(copyPath, ''))

    # 폴더에 있는 모든 파일 또는 폴더 가져옴
    for i, (path, _, files) in enumerate(os.walk(file_dir)):
        copyPath = path.split(file_dir)[-1].lstrip('/').lstrip('\\')
        # copyPath = Path(path).name
        # print(i, files, path, copyPath, _)

        # 폴더 생성
        if copyPath:
            returnDATA = createDirectory(
                os.path.join(pastePath, copyPath)
            )
            print('폴더 이름 :', returnDATA)

        # 파일 복사
        for j, file in enumerate(files):
            j += 1  # 보여주기 위함
            file_path = os.path.join(path, file)
            # 폴더안 폴더가 있을 경우
            if copyPath:
                dest_path = os.path.join(pastePath, copyPath, file)
            else:
                dest_path = os.path.join(pastePath, file)

            # 파일 복사
            shutil.copy(file_path, dest_path)

            print(
                '\t[{}_{}번째 파일 복사 완료 : {} -> {}]'.format(
                    i,
                    j,
                    file_path,
                    dest_path
                )
            )
   """

def origin_rm( copyPath:str ,dt: str):

    try :
       os.chdir(copyPath)
       os.system('find . -type f -name "*%s*" -exec rm {} \; ' % dt)

    except OSError:
        print("rm error")

def target_directory_rm(dt):

    datetime_result = datetime.strptime(dt, '%Y-%m-%d')
    rm_dt = datetime_result - timedelta(8)
    rm_dt=rm_dt.strftime('%Y-%m-%d')
    dir_path = "/DATA/pms_app/PMS_DATA/"
    name= (os.path.join(dir_path, rm_dt))
    print("rm_dt=" + name)
    if os.path.exists(name):
        os.rmdir(name)


if __name__ == "__main__":
   
   originFloder = "/home/svcapp/PMS_DATA"
   targetFloder = "/DATA/pms_app/PMS_DATA/"
   if len(sys.argv) > 1:
       dt =sys.argv[1]
       print (sys.argv[1])
   else :
 
     dt = date.today() - timedelta(1)

     print(dt.strftime('%Y-%m-%d'))

  # 해당 일자의 타켓 폴더를 생성한다.
   createDirectory(targetFloder,dt)
   # targetFolder 에서 일주일 이전 데이터 삭제
   target_directory_rm(dt)
   #origin -> targetFolder 로 복사 한다.
   folder_file_copy(originFloder,targetFloder,dt)

 
   origin_rm(originFloder,dt)
       