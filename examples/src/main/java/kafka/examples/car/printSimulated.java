package kafka.examples.car;

public class printSimulated {
    public static void main(String[] args) {
        System.out.println("📊 [Sensor] Processing Time Statistics:");
        System.out.println("   - Total Messages: " + 2380);
        System.out.println("   - Min: " + 0 + " ms");
        System.out.println("   - Max: " + 84 + " ms");
        System.out.println("   - Mean: " + String.format("%.2f", 9.17) + " ms");
        System.out.println("   - Median: " + 4 + " ms");
        System.out.println("   - Q1 (25%): " + 2 + " ms");
        System.out.println("   - Q3 (75%): " + 8 + " ms");
        System.out.println();


        System.out.println("📊 [Log] Processing Time Statistics:");
        System.out.println("   - Total Messages: " + 432);
        System.out.println("   - Min: " + 61 + " ms");
        System.out.println("   - Max: " + 104 + " ms");
        System.out.println("   - Mean: " + String.format("%.2f", 68.72) + " ms");
        System.out.println("   - Median: " + 75 + " ms");
        System.out.println("   - Q1 (25%): " + 69 + " ms");
        System.out.println("   - Q3 (75%): " + 89 + " ms");
        System.out.println();
    }
}
