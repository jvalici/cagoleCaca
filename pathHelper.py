import os


# helper to get a checked file path
def get_path( dir_name, fileType, index ):
    if index is None:
        fileName = fileType + '.csv'
    else:
        fileName = fileType + '-{}.csv'.format(index)
    return os.path.join( dir_name, fileName )
    
# helper to get a checked file path
def get_existing_path( dir_name, fileType, index ):
    filePath = get_path( dir_name, fileType, index )
    if not( os.path.isfile(filePath) ):
        raise( "missing file " + str( index ))
    else:
        return filePath
