package com.streamquote.model;

public class MarketDepth {
	public Long qty;
	public Double price;
	public Integer orders;

	/**
	 * Constructor
	 * 
	 * @param qty
	 * @param price
	 * @param orders
	 */
	public MarketDepth(Long qty, Double price, Integer orders) {
		super();
		this.qty = qty;
		this.price = price;
		this.orders = orders;
	}

	public Long getQty() {
		return qty;
	}

	public void setQty(Long qty) {
		this.qty = qty;
	}

	public Double getPrice() {
		return price;
	}

	public void setPrice(Double price) {
		this.price = price;
	}

	public Integer getOrders() {
		return orders;
	}

	public void setOrders(Integer orders) {
		this.orders = orders;
	}

	@Override
	public String toString() {
		return "MarketDepth [qty=" + qty + ", price=" + price + ", orders=" + orders + "]";
	}
}
