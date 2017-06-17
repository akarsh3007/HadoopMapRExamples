
public class NetflixDataParser {

	private String year;

	public String getYear() {
		return year;
	}

	public void setYear(String year) {
		this.year = year;
	}

	public boolean NetflixDataParse(String line) {
		String[] data = line.split(",");

		if (data[4] != null || !data[4].isEmpty()) {

			if (data[4].length() == 4) {
				if (!data[4].equalsIgnoreCase("release year")) {
					setYear(data[4]);
					return true;
				}
			}
			else
			{
				for (int i = 4; i < data.length; i++) {
					
					if(data[i].length() == 4)
					{
						setYear(data[i]);
						return true;
					}
				}
			}
		}
		return false;
	}
}
