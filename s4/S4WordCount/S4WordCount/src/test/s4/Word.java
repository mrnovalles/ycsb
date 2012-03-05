package test.s4;

public class Word {
	private String string;

	public String getString() {
		return string;
	}

	public void setString(String message) {
		this.string = message;
	}

	@Override
	public String toString() {
		return "Word [string=" + string + "]";
	}
}
