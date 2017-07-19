package com.hue.common;

public enum CardinalityType {
	ONE_TO_MANY {
		@Override
		public String getName() {
			return "One To Many";
		}
		
	},
	ONE_TO_ONE {
		@Override
		public String getName() {
			return "One To One";
		}

	},
	MANY_TO_ONE {
		@Override
		public String getName() {
			return "Many To One";
		}

	},
	MANY_TO_MANY {
		@Override
		public String getName() {
			return "Many To Many";
		}

	};
	
	public abstract String getName();
}
