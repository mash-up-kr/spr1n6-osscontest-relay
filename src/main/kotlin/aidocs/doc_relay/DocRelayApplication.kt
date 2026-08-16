package aidocs.doc_relay

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class DocRelayApplication

fun main(args: Array<String>) {
	runApplication<DocRelayApplication>(*args)
}
