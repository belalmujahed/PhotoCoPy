import hashlib
import multiprocessing
import os.path
import pathlib
import shutil
from datetime import datetime
from os import walk
from typing import List, Dict
import exif
import logging

import threading

from arg_parser import CopyArgParser

logger = logging.getLogger()
logger.setLevel(level=logging.INFO)

class PhotoCoPy:
    def __init__(self):

        self.args = CopyArgParser().args
        self.run(source=self.args.source, destinations=self.args.destinations, ignore=self.args.ignore, buffer=self.args.buffer)

    def run(self, source: str, destinations: List[str], ignore: List[str], buffer:str):
        logger.info("identifying source files")
        source_files = self.directory_files_dirpath(directory=source)
        if buffer:
            buffer_copy_threads = []
            logger.info(f"Copying files to buffer {buffer}")
            for file in source_files:
                buffer_thread = threading.Thread(target=shutil.copy, args=[file, buffer])
                buffer_thread.start()
                buffer_copy_threads.append(buffer_thread)
            [buffer_copy_thread.join() for buffer_copy_thread in buffer_copy_threads]
            orig_source_files = source_files
            source_files = self.directory_files_dirpath(directory=buffer)

            with multiprocessing.Pool() as pool:
                for dest_file, source_file in zip(source_files, orig_source_files):
                    pool.apply_async(func=self.verify_written_files, kwds={"destination_file":dest_file, "source_file":source_file})
                pool.close()
                pool.join()

        dest_folder_structure = self.get_source_dictionary(source_files=source_files)

        threads = []
        for destination in destinations:
            thread = threading.Thread(target=self.write_to_destination, kwargs={"destination_directory": destination, "dest_folder_structure":dest_folder_structure})
            thread.start()
            threads.append(thread)

        [thread.join() for thread in threads]

    def get_source_dictionary(self, source_files: List[str]) -> Dict[str,Dict[str,Dict[str,Dict[str,List[str]]]]]:
        folder_struct_dict = {}
        for path in source_files:
            with open(path, 'rb') as photo:
                try:
                    photo_data = exif.Image(photo)
                    self.process_photo(photo_data=photo_data,path=path, folder_struct_dict=folder_struct_dict)
                except Exception as e:
                    continue




        return folder_struct_dict

    @staticmethod
    def process_photo(photo_data,path:str, folder_struct_dict):
        photo_date_time = datetime.strptime(photo_data.datetime, "%Y:%m:%d %H:%M:%S")
        folder_struct_dict[photo_date_time.year] = folder_struct_dict.get(photo_date_time.year, {})
        folder_struct_dict[photo_date_time.year][photo_date_time.month] = folder_struct_dict[photo_date_time.year].get(
            photo_date_time.month, {})
        folder_struct_dict[photo_date_time.year][photo_date_time.month][photo_date_time.day] = \
        folder_struct_dict[photo_date_time.year][photo_date_time.month].get(photo_date_time.day, {})

        file_extension = path.split('.')[-1:][0]
        folder_struct_dict[photo_date_time.year][photo_date_time.month][photo_date_time.day][file_extension] = \
        folder_struct_dict[photo_date_time.year][photo_date_time.month][photo_date_time.day].get(file_extension, [])

        folder_struct_dict[photo_date_time.year][photo_date_time.month][photo_date_time.day][file_extension].append(
            path)
        logger.debug(f"Opened file: {path} with exif data: {photo_data.list_all()}")

    def write_to_destination(self,destination_directory, dest_folder_structure: Dict[str,Dict[str,Dict[str,Dict[str,List[str]]]]]):

        for year, year_data in dest_folder_structure.items():
            for month, month_data in year_data.items():
                for day, day_data in month_data.items():
                    for file_extension, file_extension_data in day_data.items():
                        path = os.path.join(str(destination_directory), str(year), str(month),str(day),file_extension)
                        if not os.path.exists(path):
                            pathlib.Path(path).mkdir(parents=True)
                        with multiprocessing.Pool() as mp_pool:
                            for file in file_extension_data:
                                source_file = pathlib.Path(file)
                                shutil.copy(source_file, path)
                                destination_file = os.path.join(path, source_file.name)

                                mp_pool.apply_async(func=self.verify_written_files, kwds={"destination_file":destination_file, "source_file":source_file})
                            mp_pool.close()
                            mp_pool.join()




    def verify_written_files(self, destination_file, source_file):
        with open(destination_file, "rb") as destination_file_, open(source_file, "rb") as source_file_:
            destination_data = destination_file_.read()
            source_data = source_file_.read()
            destination_md5 = hashlib.md5(destination_data).hexdigest()
            source_md5 = hashlib.md5(source_data).hexdigest()



            if destination_md5 == source_md5:
                logger.debug(f"{destination_file} MD5 matches {source_file} MD5")
            else:
                logger.warn(f"{destination_file} MD5 DOES NOT match {source_file} MD5")





    def directory_files_dirpath(self, directory: str):
        """
        Returns all the files located in a directory
        """

        paths = []
        for (dirpath, dirnames, filenames) in walk(directory):
            paths.extend([os.path.join(dirpath, filename) for filename in filenames])
        return paths


if __name__ == "__main__":
    PhotoCoPy()



