package de.dentrassi.kapua.dtms.poc1;

import jline.console.ConsoleReader;

public class Application {
	public static void main(String[] args) throws Exception {

		new ExternalListener();

		ServiceA service = new ServiceAImpl();

		try (ConsoleReader reader = new ConsoleReader()) {
			while (true) {
				final String cmd = reader.readLine("poc1> ");
				switch (cmd) {
				case "true":
				case "error":
				case "fail":
					service.doStuff(true);
					break;
				case "ok":
				case "false":
					service.doStuff(false);
					break;
				case "exit":
					System.exit(0);
					return;
				default:
					System.out.println("Unknown command: " + cmd);
					break;
				}
			}
		}

	}
}
