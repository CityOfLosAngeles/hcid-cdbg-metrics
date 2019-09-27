# Functions that need to be used across notebooks

def create_geoid(df):
    df = df[df.tract==6037143700]
    # Create GEOID, must come out to 11 characters
    df.GEOID = df.GEOID.astype(str)
    df.GEOID = df.GEOID.str.zfill(11)
    # Make df wide
    # Create list where the columns with "_" at some place in the column name are selected.
    oldcols = [x for x in df.columns if x.find('_') > 0]
    # Create dictionary where old column names get renamed starting from the character "_" to the end
    newcols = {x: x[x.find('_') + 1: ] for x in oldcols}
    df.rename(columns = newcols, inplace = True)
    df.drop(columns = ['state', 'county', 'tract', 'NAME'], inplace = True)