package sn_training;

import java.time.Instant;

public class Order {
    private String uniqueCustomerIdentifier;
    private String uniqueOrderNumber;
    private String country;
    private String item;
    private Integer quantity;
    private Instant orderTime;
    private String email;
    private Integer orderTotal;

    Order() {
        this.orderTime = Instant.now();
        this.item = "widget";
        this.quantity = 10;
        this.email = "me@mydomain.com";
        //this.orderTotal = 100;
    }

    public String getUniqueCustomerIdentifier() {
        return uniqueCustomerIdentifier;
    }

    public void setUniqueCustomerIdentifier(String UCI) {
        this.uniqueCustomerIdentifier = UCI;
    }

    public String getUniqueOrderNumber() {
        return uniqueOrderNumber;
    }

    public void setUniqueOrderNumber(String UON) {
        this.uniqueOrderNumber = UON;
    }

    public String getItem() {
        return item;
    }

    public void setItem(String it) {
        this.item = it;
    }

    public Integer getQuantity() {
        return quantity;
    }

    public void setQuantity(Integer quant) {
        this.quantity = quant;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String em) {
        this.email = em;
    }


    public Integer getOrderTotal() {
        return orderTotal;
    }

    public void setOrderTotal(Integer total) {
        this.orderTotal = total;
    }


    @Override
    public String toString() {
        return "Order{" +
                "uniqueCustomerIdentifier='" + uniqueCustomerIdentifier + '\'' +
                ", uniqueOrderNumber='" + uniqueOrderNumber + '\'' +
                ", country='" + country + '\'' +
                ", item='" + item + '\'' +
                ", quantity=" + quantity +
                ", orderTime=" + orderTime +
                ", email='" + email + '\'' +
                //", orderTotal=" + orderTotal +
                '}';
    }
}