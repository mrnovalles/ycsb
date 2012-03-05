package test.s4;

public class Sentence {
	private String string;

	public Sentence(){
		// default constructor
	}
	
	public Sentence(String string) {
		this.string = string;
	}

	public String getString() {
		return string;
	}

	public void setString(String message) {
		this.string = message;
	}
	
	public String getSentenceId(){
		// all sentences have the same key
		return "1";
	}
	public void setSentenceId(String id){
		// do nothing
	}

	@Override
	public String toString() {
		return "Sentence [string=" + string + "]";
	}

}
