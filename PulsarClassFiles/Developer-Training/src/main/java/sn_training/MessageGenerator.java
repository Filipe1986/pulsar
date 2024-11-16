package sn_training;
import java.util.Random;

/**
 * This class is used to generate messages for the various different tasks
 */
public class MessageGenerator {
    private Integer counter;
    Random rand;

    public MessageGenerator() {
        counter = 0;
        rand = new Random();
    }

    public static class StringMessage {
        private String value;

        private StringMessage(Integer count) {
            if(count % 2 == 0) {
                this.value = count.toString() + "China";
            } else {
                this.value = count.toString() + "US";
            }
        }

        public String getValue() {
            return value;
        }
    }

    public StringMessage nextStringMessage() {
        counter++;
        return new StringMessage(counter);
    }

    public Order nextOrder() {
        counter++;
        Order myOrder = new Order();

        myOrder.setUniqueCustomerIdentifier(counter + "UCI");
        myOrder.setUniqueOrderNumber(counter + "UON");
        myOrder.setQuantity(rand.nextInt(10));
        System.out.println("Quantity: " + myOrder.getQuantity());
        myOrder.setOrderTotal(myOrder.getQuantity() * 10);
        System.out.println("order Total: " + myOrder.getOrderTotal());
        
//        if (counter%2 == 0) {
//            myOrder.setCountry("China");
//        } else {
//            myOrder.setCountry("US");
//        };
//
        //use with key-based exercise to add more countries

        switch(counter % 10) {
            case 0:
                myOrder.setCountry("China");
                break;
            case 1:
                myOrder.setCountry("US");
                break;
            case 2:
                myOrder.setCountry("Brazil");
                break;
            case 3:
                myOrder.setCountry("Japan");
                break;
            case 4:
                myOrder.setCountry("Panama");
                break;
            case 5:
                myOrder.setCountry("Singapore");
                break;
            case 6:
                myOrder.setCountry("France");
                break;
            case 7:
                myOrder.setCountry("South Africa");
                break;
            case 8:
                myOrder.setCountry("Australia");
                break;
            case 9:
                myOrder.setCountry("South Korea");
                break;
        }

        return myOrder;
    }
}