#ifndef __SMACK_FILTER_HPP
#define __SMACK_FILTER_HPP

namespace ioremap { namespace smack {

class filter {
	public:
		virtual run(std::vector<char> &) = 0;
};

}}

#endif /* __SMACK_FILTER_HPP */
