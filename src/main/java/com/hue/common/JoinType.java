/*
 * Vero Analytics
 */

package com.hue.common;


/**
 * 
 * @author Tai Hu
 */
public enum JoinType {
	INNER_JOIN {
		@Override
		public JoinType next() {
			return FULL_OUTER_JOIN;
		}

		@Override
		public String getName() {
			return "Inner Join";
		}
	},
	FULL_OUTER_JOIN {
		@Override
		public JoinType next() {
			return CROSS_JOIN;
		}

		@Override
		public String getName() {
			return "Full Outer Join";
		}

	},
	CROSS_JOIN {
		@Override
		public JoinType next() {
			return LEFT_OUTER_JOIN;
		}

		@Override
		public String getName() {
			return "Cross Join";
		}

	},
	LEFT_OUTER_JOIN {
		@Override
		public JoinType next() {
			return RIGHT_OUTER_JOIN;
		}

		@Override
		public String getName() {
			return "Left Outer Join";
		}

	},
	RIGHT_OUTER_JOIN {
		@Override
		public JoinType next() {
			return INNER_JOIN;
		}

		@Override
		public String getName() {
			return "Right Outer Join";
		}
	};

	public abstract JoinType next();
	public abstract String getName();
}
