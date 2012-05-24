#include <fstream>

#include <smack/smack.hpp>

using namespace ioremap::smack;

int main(int argc, char *argv[])
{
	for (int i = 1; i < argc; ++i) {
		std::ifstream in(argv[i]);

		struct index idx;

		while (!in.eof() && in.good()) {
			in.read((char *)&idx, sizeof(struct index));

			if (!in.good())
				break;

			key key(idx.id, SMACK_KEY_SIZE);

			std::cout << key.str() << ": data-offset: " << idx.data_offset << ", data-size: " << idx.data_size << std::endl;
		}
	}
}
