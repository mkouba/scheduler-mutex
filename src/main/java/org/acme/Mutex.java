package org.acme;

import io.quarkus.hibernate.orm.panache.PanacheEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Version;

@Entity
public class Mutex extends PanacheEntity {

    @Column(unique = true, nullable = false)
    public String identity;
    
    public String lockOwner;

    @Version
    private int version;

}
