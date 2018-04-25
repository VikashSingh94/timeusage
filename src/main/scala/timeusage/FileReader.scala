package timeusage

import java.io.File

object FileReader {

  def filePath: String = {
    val resource = this.getClass.getClassLoader.getResource("airline/train_df.csv")
    if (resource == null) sys.error("No data set present")
    new File(resource.toURI).getPath
  }
}
